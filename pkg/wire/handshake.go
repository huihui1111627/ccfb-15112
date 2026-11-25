package wire

import (
	"crypto/ecdh"
	"errors"
	"fmt"
)

// ClientKey generates an X25519 ephemeral key.
func ClientKey() (*ecdh.PrivateKey, error) {
	return ecdh.X25519().GenerateKey(nil)
}

// EncodeSYN encodes the initial handshake offer.
// Body: client nonce || X25519 public value.
func EncodeSYN(connID uint64, nonce, pub []byte) []byte {
	pkt := AppendHeader(nil, Header{Type: TypeSYN, ConnID: connID}, 0)
	pkt = append(pkt, nonce...)
	pkt = appendU16(pkt, uint16(len(pub)))
	pkt = append(pkt, pub...)
	return pkt
}

func DecodeSYN(pkt []byte) (nonce, pub []byte, err error) {
	h, err := DecodeHeader(pkt)
	if err != nil {
		return nil, nil, err
	}
	if h.Type != TypeSYN {
		return nil, nil, fmt.Errorf("expected SYN, got %s", h.Type)
	}
	b := pkt[HeaderLen:]
	if len(b) < NonceBytes+2 {
		return nil, nil, ErrShortPacket
	}
	nonce = append([]byte(nil), b[:NonceBytes]...)
	b = b[NonceBytes:]
	n, b, err := takeU16(b)
	if err != nil {
		return nil, nil, err
	}
	if int(n) != len(b) || n != 32 {
		return nil, nil, errors.New("bad X25519 public key length")
	}
	pub = append([]byte(nil), b...)
	return nonce, pub, nil
}

// EncodeSYNACK encodes the server handshake response and key confirmation.
func EncodeSYNACK(connID uint64, nonce, pub, confirmMAC []byte) []byte {
	pkt := AppendHeader(nil, Header{Type: TypeSYNACK, ConnID: connID}, 0)
	pkt = append(pkt, nonce...)
	pkt = appendU16(pkt, uint16(len(pub)))
	pkt = append(pkt, pub...)
	pkt = append(pkt, confirmMAC...)
	return pkt
}

func DecodeSYNACK(pkt []byte) (nonce, pub, mac []byte, err error) {
	h, err := DecodeHeader(pkt)
	if err != nil {
		return nil, nil, nil, err
	}
	if h.Type != TypeSYNACK {
		return nil, nil, nil, fmt.Errorf("expected SYNACK, got %s", h.Type)
	}
	b := pkt[HeaderLen:]
	if len(b) < NonceBytes+2+32+JoinMACBytes {
		return nil, nil, nil, ErrShortPacket
	}
	nonce = append([]byte(nil), b[:NonceBytes]...)
	b = b[NonceBytes:]
	n, b, err := takeU16(b)
	if err != nil {
		return nil, nil, nil, err
	}
	if n != 32 || len(b) != 32+JoinMACBytes {
		return nil, nil, nil, errors.New("bad SYNACK key/MAC length")
	}
	pub = append([]byte(nil), b[:32]...)
	mac = append([]byte(nil), b[32:32+JoinMACBytes]...)
	return nonce, pub, mac, nil
}

// EncodeHandshakeACK is the client's key confirmation.
func EncodeHandshakeACK(connID uint64, mac []byte) []byte {
	pkt := AppendHeader(nil, Header{Type: TypeHandshakeACK, ConnID: connID}, 0)
	return append(pkt, mac...)
}

func DecodeMACOnly(pkt []byte, want PacketType, macLen int) (Header, []byte, error) {
	h, err := DecodeHeader(pkt)
	if err != nil {
		return Header{}, nil, err
	}
	if h.Type != want {
		return h, nil, fmt.Errorf("expected %s, got %s", want, h.Type)
	}
	if len(pkt) != HeaderLen+macLen {
		return h, nil, fmt.Errorf("%w: %s mac length", ErrAuth, want)
	}
	return h, pkt[HeaderLen:], nil
}

// JoinBody is the authenticated request to add a path.
type JoinBody struct {
	PathID   uint16
	Priority uint8
	Token    []byte
	MAC      []byte
}

func EncodeJOIN(connID uint64, subflowID uint16, b JoinBody) []byte {
	h := Header{Type: TypeJOIN, ConnID: connID, PathID: b.PathID, SubflowID: subflowID}
	pkt := AppendHeader(nil, h, 0)
	pkt = appendU16(pkt, b.PathID)
	pkt = append(pkt, b.Priority)
	if len(b.Token) != TokenBytes {
		panic("bad join token length")
	}
	pkt = append(pkt, b.Token...)
	if len(b.MAC) != JoinMACBytes {
		panic("bad join MAC length")
	}
	pkt = append(pkt, b.MAC...)
	return pkt
}

func DecodeJOIN(pkt []byte) (Header, JoinBody, error) {
	h, err := DecodeHeader(pkt)
	if err != nil {
		return h, JoinBody{}, err
	}
	if h.Type != TypeJOIN {
		return h, JoinBody{}, fmt.Errorf("expected JOIN, got %s", h.Type)
	}
	b := pkt[HeaderLen:]
	pathID, b, err := takeU16(b)
	if err != nil {
		return h, JoinBody{}, err
	}
	if len(b) < 1+TokenBytes+JoinMACBytes {
		return h, JoinBody{}, ErrShortPacket
	}
	priority := b[0]
	b = b[1:]
	token := append([]byte(nil), b[:TokenBytes]...)
	b = b[TokenBytes:]
	mac := append([]byte(nil), b[:JoinMACBytes]...)
	return h, JoinBody{PathID: pathID, Priority: priority, Token: token, MAC: mac}, nil
}

type JoinAckBody struct {
	PathID    uint16
	SubflowID uint16
	Status    uint8
	MAC       []byte
}

const (
	JoinStatusOK uint8 = iota
	JoinStatusRejectFull
	JoinStatusRejectAuth
)

func EncodeJOINACK(connID uint64, pathID, subflowID uint16, status uint8, mac []byte) []byte {
	h := Header{Type: TypeJOINACK, ConnID: connID, PathID: pathID, SubflowID: subflowID}
	pkt := AppendHeader(nil, h, 0)
	pkt = appendU16(pkt, pathID)
	pkt = appendU16(pkt, subflowID)
	pkt = append(pkt, status)
	pkt = append(pkt, mac...)
	return pkt
}

func DecodeJOINACK(pkt []byte) (Header, JoinAckBody, error) {
	h, err := DecodeHeader(pkt)
	if err != nil {
		return h, JoinAckBody{}, err
	}
	if h.Type != TypeJOINACK {
		return h, JoinAckBody{}, fmt.Errorf("expected JOINACK, got %s", h.Type)
	}
	b := pkt[HeaderLen:]
	pathID, b, err := takeU16(b)
	if err != nil {
		return h, JoinAckBody{}, err
	}
	subflowID, b, err := takeU16(b)
	if err != nil {
		return h, JoinAckBody{}, err
	}
	if len(b) < 1+JoinMACBytes {
		return h, JoinAckBody{}, ErrShortPacket
	}
	status := b[0]
	mac := append([]byte(nil), b[1:1+JoinMACBytes]...)
	return h, JoinAckBody{PathID: pathID, SubflowID: subflowID, Status: status, MAC: mac}, nil
}
