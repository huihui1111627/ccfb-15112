package wire

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

// Protocol constants. The protocol is intentionally framed and self-contained
// so the simulated network can drop, delay, split and mutate complete records.
var (
	Magic                 = [4]byte{'C', 'M', 'P', '1'}
	Version         uint8 = 1
	HeaderLen             = 20
	MaxPacket             = 64 << 10
	MappingMACBytes       = 12
	JoinMACBytes          = 16
	TokenBytes            = 16
	NonceBytes            = 16
)

// PacketType identifies a framed protocol record.
type PacketType uint8

const (
	TypeSYN PacketType = iota + 1
	TypeSYNACK
	TypeHandshakeACK
	TypeJOIN
	TypeJOINACK
	TypeData
	TypeSubflowACK
	TypeConnectionACK
	TypeHeartbeat
	TypeFallback
	TypeFIN
	TypeFINACK
	TypeSubflowClose
)

func (t PacketType) String() string {
	switch t {
	case TypeSYN:
		return "SYN"
	case TypeSYNACK:
		return "SYNACK"
	case TypeHandshakeACK:
		return "HACK"
	case TypeJOIN:
		return "JOIN"
	case TypeJOINACK:
		return "JOINACK"
	case TypeData:
		return "DATA"
	case TypeSubflowACK:
		return "SUB-ACK"
	case TypeConnectionACK:
		return "CONN-ACK"
	case TypeHeartbeat:
		return "HEARTBEAT"
	case TypeFallback:
		return "FALLBACK"
	case TypeFIN:
		return "FIN"
	case TypeFINACK:
		return "FIN-ACK"
	case TypeSubflowClose:
		return "SUB-CLOSE"
	default:
		return fmt.Sprintf("UNKNOWN-%d", uint8(t))
	}
}

// Flags carried in the fixed header.
const (
	FlagRetransmission uint8 = 1 << iota
	FlagRedundant
	FlagProbe
)

// Range is a half-open sequence interval [Start, End).
type Range struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

func (r Range) Len() uint64 {
	if r.End <= r.Start {
		return 0
	}
	return r.End - r.Start
}

// Header is the fixed 20-byte record header.
type Header struct {
	Type      PacketType
	Flags     uint8
	PathID    uint16
	SubflowID uint16
	ConnID    uint64
}

func AppendHeader(dst []byte, h Header, bodyLen int) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, HeaderLen)...)
	copy(dst[start:start+4], Magic[:])
	dst[start+4] = Version
	dst[start+5] = byte(h.Type)
	dst[start+6] = h.Flags
	dst[start+7] = 0
	binary.LittleEndian.PutUint16(dst[start+8:start+10], h.PathID)
	binary.LittleEndian.PutUint16(dst[start+10:start+12], h.SubflowID)
	binary.LittleEndian.PutUint64(dst[start+12:start+20], h.ConnID)
	_ = bodyLen
	return dst
}

func DecodeHeader(pkt []byte) (Header, error) {
	if len(pkt) < HeaderLen {
		return Header{}, fmt.Errorf("%w: short header: %d bytes", ErrShortPacket, len(pkt))
	}
	if string(pkt[0:4]) != string(Magic[:]) {
		return Header{}, ErrBadMagic
	}
	if pkt[4] != Version {
		return Header{}, fmt.Errorf("%w: version %d", ErrBadVersion, pkt[4])
	}
	return Header{
		Type:      PacketType(pkt[5]),
		Flags:     pkt[6],
		PathID:    binary.LittleEndian.Uint16(pkt[8:10]),
		SubflowID: binary.LittleEndian.Uint16(pkt[10:12]),
		ConnID:    binary.LittleEndian.Uint64(pkt[12:20]),
	}, nil
}

var (
	ErrShortPacket  = errors.New("short packet")
	ErrBadMagic     = errors.New("bad protocol magic")
	ErrBadVersion   = errors.New("bad protocol version")
	ErrMappingCheck = errors.New("mapping verification failed")
	ErrAuth         = errors.New("authentication failed")
)

func appendU16(dst []byte, v uint16) []byte {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], v)
	return append(dst, b[:]...)
}

func appendU32(dst []byte, v uint32) []byte {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	return append(dst, b[:]...)
}

func appendU64(dst []byte, v uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return append(dst, b[:]...)
}

func takeU16(b []byte) (uint16, []byte, error) {
	if len(b) < 2 {
		return 0, nil, ErrShortPacket
	}
	return binary.LittleEndian.Uint16(b[:2]), b[2:], nil
}

func takeU32(b []byte) (uint32, []byte, error) {
	if len(b) < 4 {
		return 0, nil, ErrShortPacket
	}
	return binary.LittleEndian.Uint32(b[:4]), b[4:], nil
}

func takeU64(b []byte) (uint64, []byte, error) {
	if len(b) < 8 {
		return 0, nil, ErrShortPacket
	}
	return binary.LittleEndian.Uint64(b[:8]), b[8:], nil
}

// RandomBytes returns n cryptographically random bytes.
func RandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	return b, err
}

// RandomUint64 returns a random connection identifier.
func RandomUint64() (uint64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b[:]), nil
}

// ---- Key schedule ---------------------------------------------------------

// Keys contains the derived connection keying material.
type Keys struct {
	Master []byte
	MacKey []byte
	Token  []byte
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(data)
	return h.Sum(nil)
}

// hkdfSingle implements the small HKDF subset needed here without external
// dependencies.
func hkdfSingle(secret, salt, info []byte, n int) []byte {
	prk := hmacSHA256(salt, secret)
	mac := hmac.New(sha256.New, prk)
	_, _ = mac.Write(info)
	_, _ = mac.Write([]byte{0x01})
	out := mac.Sum(nil)
	return out[:n]
}

// DeriveKeys performs the HKDF key schedule from ECDH shared secret and
// nonces. The token binds future JOIN attempts to this connection.
func DeriveKeys(shared, clientNonce, serverNonce, connContext []byte) (*Keys, error) {
	salt := make([]byte, 0, len(clientNonce)+len(serverNonce))
	salt = append(salt, clientNonce...)
	salt = append(salt, serverNonce...)
	master := hkdfSingle(shared, salt, []byte("clocktower-mp-master-v1"), 32)
	macKey := hkdfSingle(master, nil, []byte("clocktower-mp-mac-v1"), 32)
	token := make([]byte, TokenBytes)
	mac := hmac.New(sha256.New, master)
	_, _ = mac.Write([]byte("join-token-v1"))
	_, _ = mac.Write(connContext)
	copy(token, mac.Sum(nil)[:TokenBytes])
	return &Keys{Master: master, MacKey: macKey, Token: token}, nil
}

// TruncatedMAC returns the first n bytes of HMAC-SHA256.
func TruncatedMAC(key []byte, n int, parts ...[]byte) []byte {
	h := hmac.New(sha256.New, key)
	for _, p := range parts {
		_, _ = h.Write(p)
	}
	sum := h.Sum(nil)
	out := make([]byte, n)
	copy(out, sum[:n])
	return out
}

func EqualMAC(a, b []byte) bool {
	return hmac.Equal(a, b)
}
