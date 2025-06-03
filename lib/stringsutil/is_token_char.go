package stringsutil

import (
	"unicode"
)

func IsTokenCharSlow(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_'
}

func IsTokenChar(c byte) bool {
	return tokenCharTable[c] != 0
}

var tokenCharTable [256]byte = initTokenCharTable()

func initTokenCharTable() (table [256]byte) {
	for c := 'a'; c <= 'z'; c++ {
		table[c] = 0xff
	}
	for c := 'A'; c <= 'Z'; c++ {
		table[c] = 0xff
	}
	for c := '0'; c <= '9'; c++ {
		table[c] = 0xff
	}
	table['_'] = 0xff
	return
}

func IsTokenRuneSlow(c rune) bool {
	return unicode.IsLetter(c) || unicode.IsDigit(c) || c == '_'
}

const unicodeCharCount = 65536

var unicodeTokenTable [unicodeCharCount / 8]byte = initUnicodeTokenCharTable()

func initUnicodeTokenCharTable() (table [unicodeCharCount / 8]byte) {
	for i := 0; i < unicodeCharCount; i++ {
		r := rune(i)
		if IsTokenRuneSlow(r) {
			byteIndex := i / 8
			bitIndex := i & 7
			table[byteIndex] |= (1 << bitIndex)
		}
	}
	return
}

func IsTokenRune(c rune) bool {
	if c < unicodeCharCount {
		return (unicodeTokenTable[c/8] & (1 << (c & 7))) != 0
	}
	return unicode.IsLetter(c) || unicode.IsDigit(c)
}
