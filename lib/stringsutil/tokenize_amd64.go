package stringsutil

// tokenToBitmap tokenize string, and set bit to bitmap
// @param s source string
// @param asciiTable global variable `tokenCharTable`
// @param[out] outBitmap1 bitmap for all tokens, must align to 32 bits, min length is (len(s)+7)/8
// @param[out] outBitmap2 bitmap for unicode tokens. must align to 32 bits, min length is (len(s)+7)/8
//func tokenToBitmap(s string, asciiTable *uint8, outBitmap1 *uint8, outBitmap2 *uint8)
