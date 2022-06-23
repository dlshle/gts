package tcp

import (
	"encoding/binary"
	"github.com/dlshle/gommon/errors"
)

/*
 * TCP Message Format:
 * |Payload length(4)|Version(1)|CheckSum(1)|Payload...|
 * ~Payload length is a 4-byte BigEndian unsigned int byte stream
 * ~Version is a one-byte unsigned int number
 * ~Check sum is a one-byte number calculated by (Xor(each byte of payload length) Or (Version))
 */

const headerLength = 6

func wrapData(data []byte, version byte) []byte {
	header := make([]byte, headerLength, headerLength)
	binary.BigEndian.PutUint32(header, uint32(len(data)))
	header[4] = version
	header[5] = hashCheckSum(header[:4], version)
	return append(header, data...)
}

func parseMessageHeader(version byte, header []byte) (uint32, error) {
	if len(header) != headerLength {
		return 0, errors.Error("invalid header length")
	}
	messageVer := header[4]
	if version != messageVer {
		return 0, errors.Error("invalid message version")
	}
	length := binary.BigEndian.Uint32(header[:4])
	checkSum := header[5]
	if checkSum != hashCheckSum(header[:4], version) {
		return 0, errors.Error("invalid message format")
	}
	return length, nil
}

func hashCheckSum(lengthBytes []byte, version byte) byte {
	var cs byte = 0
	for _, b := range lengthBytes {
		cs ^= b
	}
	cs |= version
	return cs
}
