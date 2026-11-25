package wire

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	OptMapping uint8 = 1
	OptTS      uint8 = 2
)

// Mapping maps a subflow byte-number interval to the shared connection data
// number interval. All intervals are half-open.
type Mapping struct {
	PathID   uint16
	DSNStart uint64
	DSNLen   uint32
	SSNStart uint64
	SSNLen   uint32
	MapCRC   uint32
	MapMAC   []byte
	Verified bool
}

func (m Mapping) DSNEnd() uint64 { return m.DSNStart + uint64(m.DSNLen) }
func (m Mapping) SSNEnd() uint64 { return m.SSNStart + uint64(m.SSNLen) }

func mappingSignedBytes(m Mapping) []byte {
	b := make([]byte, 0, 26)
	b = appendU16(b, m.PathID)
	b = appendU64(b, m.DSNStart)
	b = appendU32(b, m.DSNLen)
	b = appendU64(b, m.SSNStart)
	b = appendU32(b, m.SSNLen)
	return b
}

func mappingMAC(key []byte, connID uint64, signed, crcBytes []byte) []byte {
	var cid [8]byte
	binary.LittleEndian.PutUint64(cid[:], connID)
	return TruncatedMAC(key, MappingMACBytes,
		[]byte("clocktower-mapping-v1"), cid[:], signed, crcBytes)
}

// EncodeData builds an authenticated mapping option followed by payload.
func EncodeData(h Header, m Mapping, payload, macKey []byte) []byte {
	if uint32(len(payload)) != m.DSNLen || uint32(len(payload)) != m.SSNLen {
		panic("mapping length must equal payload length")
	}
	h.Type = TypeData
	h.PathID = m.PathID

	signed := mappingSignedBytes(m)
	m.MapCRC = crc32.ChecksumIEEE(signed)
	var crcBytes [4]byte
	binary.LittleEndian.PutUint32(crcBytes[:], m.MapCRC)
	m.MapMAC = mappingMAC(macKey, h.ConnID, signed, crcBytes[:])

	opt := make([]byte, 0, 3+len(signed)+4+MappingMACBytes)
	opt = append(opt, OptMapping)
	opt = appendU16(opt, uint16(3+len(signed)+4+MappingMACBytes))
	opt = append(opt, signed...)
	opt = appendU32(opt, m.MapCRC)
	opt = append(opt, m.MapMAC...)

	pkt := AppendHeader(nil, h, 2+len(opt)+len(payload))
	pkt = appendU16(pkt, uint16(len(opt)))
	pkt = append(pkt, opt...)
	pkt = append(pkt, payload...)
	return pkt
}

// DecodeData validates framing, length, CRC and keyed mapping MAC.
func DecodeData(pkt, macKey []byte) (Header, Mapping, []byte, error) {
	h, err := DecodeHeader(pkt)
	if err != nil {
		return h, Mapping{}, nil, err
	}
	if h.Type != TypeData {
		return h, Mapping{}, nil, fmt.Errorf("expected DATA, got %s", h.Type)
	}
	if len(pkt) < HeaderLen+2 {
		return h, Mapping{}, nil, fmt.Errorf("%w: missing options length", ErrMappingCheck)
	}
	optLen := int(binary.LittleEndian.Uint16(pkt[HeaderLen : HeaderLen+2]))
	optStart := HeaderLen + 2
	payloadStart := optStart + optLen
	if optLen < 3 || payloadStart > len(pkt) {
		return h, Mapping{}, nil, fmt.Errorf("%w: truncated options", ErrMappingCheck)
	}
	opts := pkt[optStart:payloadStart]
	if opts[0] != OptMapping {
		return h, Mapping{}, nil, fmt.Errorf("%w: missing mapping option", ErrMappingCheck)
	}
	if len(opts) < 3 {
		return h, Mapping{}, nil, fmt.Errorf("%w: short mapping option", ErrMappingCheck)
	}
	mlen := int(binary.LittleEndian.Uint16(opts[1:3]))
	if mlen != len(opts) || mlen < 30 {
		return h, Mapping{}, nil, fmt.Errorf("%w: bad mapping option length %d/%d", ErrMappingCheck, mlen, len(opts))
	}
	b := opts[3:]
	pathID16, b, err := takeU16(b)
	if err != nil {
		return h, Mapping{}, nil, fmt.Errorf("%w: path: %v", ErrMappingCheck, err)
	}
	dsn, b, err := takeU64(b)
	if err != nil {
		return h, Mapping{}, nil, fmt.Errorf("%w: dsn: %v", ErrMappingCheck, err)
	}
	dsnLen, b, err := takeU32(b)
	if err != nil {
		return h, Mapping{}, nil, fmt.Errorf("%w: dsnlen: %v", ErrMappingCheck, err)
	}
	ssn, b, err := takeU64(b)
	if err != nil {
		return h, Mapping{}, nil, fmt.Errorf("%w: ssn: %v", ErrMappingCheck, err)
	}
	ssnLen, b, err := takeU32(b)
	if err != nil {
		return h, Mapping{}, nil, fmt.Errorf("%w: ssnlen: %v", ErrMappingCheck, err)
	}
	wantSignedLen := 2 + 8 + 4 + 8 + 4
	if len(b) != 4+MappingMACBytes || mlen != 3+wantSignedLen+4+MappingMACBytes {
		return h, Mapping{}, nil, fmt.Errorf("%w: mapping trailing length", ErrMappingCheck)
	}
	signed := opts[3 : 3+wantSignedLen]
	crc := binary.LittleEndian.Uint32(b[:4])
	gotCRC := crc32.ChecksumIEEE(signed)
	if crc != gotCRC {
		return h, Mapping{}, nil, fmt.Errorf("%w: crc mismatch", ErrMappingCheck)
	}
	mac := b[4 : 4+MappingMACBytes]
	var crcBytes [4]byte
	binary.LittleEndian.PutUint32(crcBytes[:], crc)
	wantMAC := mappingMAC(macKey, h.ConnID, signed, crcBytes[:])
	if !EqualMAC(mac, wantMAC) {
		return h, Mapping{}, nil, fmt.Errorf("%w: truncated MAC mismatch", ErrMappingCheck)
	}
	payload := pkt[payloadStart:]
	if uint64(len(payload)) != uint64(dsnLen) || ssnLen != dsnLen {
		return h, Mapping{}, nil, fmt.Errorf("%w: payload/mapping length mismatch", ErrMappingCheck)
	}
	m := Mapping{
		PathID: pathID16, DSNStart: dsn, DSNLen: dsnLen,
		SSNStart: ssn, SSNLen: ssnLen, MapCRC: crc,
		MapMAC: append([]byte(nil), mac...), Verified: true,
	}
	if m.PathID != h.PathID {
		return h, m, payload, fmt.Errorf("%w: path id disagrees with header", ErrMappingCheck)
	}
	return h, m, payload, nil
}

// MutateMappingOption flips a DSN byte inside the signed mapping option. It
// models a middlebox that rewrites an option without the connection key.
func MutateMappingOption(pkt []byte) []byte {
	out := append([]byte(nil), pkt...)
	if len(out) < HeaderLen+2+3+2+8 {
		return out
	}
	opt := HeaderLen + 2
	// kind(1)+length(2)+pathID(2), then low DSN byte.
	pos := opt + 3 + 2
	out[pos] ^= 0xFF
	return out
}

// SplitMappingPacket cuts a DATA record inside its mapping option. The first
// fragment fails mapping validation; the second has no valid fixed header.
func SplitMappingPacket(pkt []byte) [][]byte {
	cut := HeaderLen + 2 + 8
	if cut >= len(pkt) {
		cut = len(pkt) / 2
	}
	return [][]byte{append([]byte(nil), pkt[:cut]...), append([]byte(nil), pkt[cut:]...)}
}

// ---- ACK and control records ---------------------------------------------

func appendRanges(dst []byte, ranges []Range) []byte {
	if len(ranges) > 255 {
		ranges = ranges[:255]
	}
	dst = append(dst, byte(len(ranges)))
	for _, r := range ranges {
		dst = appendU64(dst, r.Start)
		dst = appendU64(dst, r.End)
	}
	return dst
}

func takeRanges(b []byte) ([]Range, []byte, error) {
	if len(b) < 1 {
		return nil, nil, ErrShortPacket
	}
	n := int(b[0])
	b = b[1:]
	out := make([]Range, 0, n)
	for i := 0; i < n; i++ {
		start, rest, err := takeU64(b)
		if err != nil {
			return nil, nil, err
		}
		end, rest, err := takeU64(rest)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, Range{Start: start, End: end})
		b = rest
	}
	return out, b, nil
}

func EncodeSubflowACK(connID uint64, pathID, subflowID uint16, cumSSN uint64, sacked []Range) []byte {
	h := Header{Type: TypeSubflowACK, ConnID: connID, PathID: pathID, SubflowID: subflowID}
	pkt := AppendHeader(nil, h, 0)
	pkt = appendU64(pkt, cumSSN)
	pkt = appendRanges(pkt, sacked)
	return pkt
}

func DecodeSubflowACK(pkt []byte) (h Header, cum uint64, ranges []Range, err error) {
	h, err = DecodeHeader(pkt)
	if err != nil {
		return h, 0, nil, err
	}
	if h.Type != TypeSubflowACK {
		return h, 0, nil, fmt.Errorf("expected SUBACK, got %s", h.Type)
	}
	b := pkt[HeaderLen:]
	cum, b, err = takeU64(b)
	if err != nil {
		return h, 0, nil, err
	}
	ranges, _, err = takeRanges(b)
	return h, cum, ranges, err
}

func EncodeConnectionACK(connID uint64, cumDSN uint64, rwnd uint32, sacked []Range) []byte {
	h := Header{Type: TypeConnectionACK, ConnID: connID}
	pkt := AppendHeader(nil, h, 0)
	pkt = appendU64(pkt, cumDSN)
	pkt = appendU32(pkt, rwnd)
	pkt = appendRanges(pkt, sacked)
	return pkt
}

func DecodeConnectionACK(pkt []byte) (h Header, cum uint64, rwnd uint32, ranges []Range, err error) {
	h, err = DecodeHeader(pkt)
	if err != nil {
		return h, 0, 0, nil, err
	}
	if h.Type != TypeConnectionACK {
		return h, 0, 0, nil, fmt.Errorf("expected CONNACK, got %s", h.Type)
	}
	b := pkt[HeaderLen:]
	cum, b, err = takeU64(b)
	if err != nil {
		return h, 0, 0, nil, err
	}
	rwnd, b, err = takeU32(b)
	if err != nil {
		return h, 0, 0, nil, err
	}
	ranges, _, err = takeRanges(b)
	return h, cum, rwnd, ranges, err
}

func EncodeSimple(t PacketType, connID uint64, pathID, subflowID uint16, body []byte) []byte {
	pkt := AppendHeader(nil, Header{Type: t, ConnID: connID, PathID: pathID, SubflowID: subflowID}, 0)
	return append(pkt, body...)
}

func EncodeFallback(connID uint64, reason uint8, macKey []byte) []byte {
	var cid [8]byte
	binary.LittleEndian.PutUint64(cid[:], connID)
	mac := TruncatedMAC(macKey, JoinMACBytes, []byte("fallback-v1"), cid[:], []byte{reason})
	body := []byte{reason}
	body = append(body, mac...)
	return EncodeSimple(TypeFallback, connID, 0, 0, body)
}

func DecodeFallback(pkt, macKey []byte) (reason uint8, err error) {
	h, err := DecodeHeader(pkt)
	if err != nil {
		return 0, err
	}
	if h.Type != TypeFallback {
		return 0, fmt.Errorf("expected FALLBACK, got %s", h.Type)
	}
	if len(pkt) != HeaderLen+1+JoinMACBytes {
		return 0, ErrAuth
	}
	reason = pkt[HeaderLen]
	mac := pkt[HeaderLen+1:]
	var cid [8]byte
	binary.LittleEndian.PutUint64(cid[:], h.ConnID)
	want := TruncatedMAC(macKey, JoinMACBytes, []byte("fallback-v1"), cid[:], []byte{reason})
	if !EqualMAC(mac, want) {
		return 0, ErrAuth
	}
	return reason, nil
}

func EncodeFIN(connID uint64, finalDSN uint64) []byte {
	body := appendU64(nil, finalDSN)
	return EncodeSimple(TypeFIN, connID, 0, 0, body)
}

func DecodeFIN(pkt []byte) (uint64, error) {
	h, err := DecodeHeader(pkt)
	if err != nil {
		return 0, err
	}
	if h.Type != TypeFIN {
		return 0, fmt.Errorf("expected FIN, got %s", h.Type)
	}
	if len(pkt) != HeaderLen+8 {
		return 0, ErrShortPacket
	}
	return binary.LittleEndian.Uint64(pkt[HeaderLen:]), nil
}
