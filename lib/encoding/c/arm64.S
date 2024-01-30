	.section	__TEXT,__text,regular,pure_instructions
	.build_version macos, 14, 0	sdk_version 14, 2
	.section	__TEXT,__literal16,16byte_literals
	.p2align	4, 0x0                          ; -- Begin function MarshalVarInt64s
lCPI0_0:
	.quad	-21                             ; 0xffffffffffffffeb
	.quad	-28                             ; 0xffffffffffffffe4
lCPI0_1:
	.quad	-7                              ; 0xfffffffffffffff9
	.quad	-14                             ; 0xfffffffffffffff2
lCPI0_2:
	.quad	-49                             ; 0xffffffffffffffcf
	.quad	-56                             ; 0xffffffffffffffc8
lCPI0_3:
	.quad	-35                             ; 0xffffffffffffffdd
	.quad	-42                             ; 0xffffffffffffffd6
	.section	__TEXT,__text,regular,pure_instructions
	.globl	_MarshalVarInt64s
	.p2align	2
_MarshalVarInt64s:                      ; @MarshalVarInt64s
	.cfi_startproc
; %bb.0:
                                        ; kill: def $w2 killed $w2 def $x2
	cmp	w4, #1
	b.lt	LBB0_19
; %bb.1:
	sxtw	x8, w2
	mov	w9, #64
	mov	w10, #37
Lloh0:
	adrp	x11, lCPI0_0@PAGE
Lloh1:
	ldr	q0, [x11, lCPI0_0@PAGEOFF]
Lloh2:
	adrp	x11, lCPI0_1@PAGE
Lloh3:
	ldr	q1, [x11, lCPI0_1@PAGEOFF]
	mov	w11, w4
Lloh4:
	adrp	x12, lCPI0_2@PAGE
Lloh5:
	ldr	q2, [x12, lCPI0_2@PAGEOFF]
Lloh6:
	adrp	x12, lCPI0_3@PAGE
Lloh7:
	ldr	q3, [x12, lCPI0_3@PAGEOFF]
	movi.8b	v4, #128
Lloh8:
	adrp	x12, lJTI0_0@PAGE
Lloh9:
	add	x12, x12, lJTI0_0@PAGEOFF
	b	LBB0_5
LBB0_2:                                 ;   in Loop: Header=BB0_5 Depth=1
	strb	wzr, [x0], #1
LBB0_3:                                 ;   in Loop: Header=BB0_5 Depth=1
	add	x1, x1, #8
LBB0_4:                                 ;   in Loop: Header=BB0_5 Depth=1
	add	x3, x3, #8
	subs	x11, x11, #1
	b.eq	LBB0_19
LBB0_5:                                 ; =>This Inner Loop Header: Depth=1
	ldr	x13, [x1]
	add	x13, x13, #10
	cmp	x13, x8
	b.hs	LBB0_19
; %bb.6:                                ;   in Loop: Header=BB0_5 Depth=1
	ldr	x13, [x3]
	lsl	x14, x13, #1
	eor	x13, x14, x13, asr #63
	cbz	x13, LBB0_2
; %bb.7:                                ;   in Loop: Header=BB0_5 Depth=1
	lsr	x14, x13, #1
	clz	x14, x14
	sub	w14, w9, w14
	and	w15, w14, #0xff
	mul	w15, w15, w10
	lsr	w15, w15, #8
	sub	w14, w14, w15
	and	w14, w14, #0xfe
	add	w14, w15, w14, lsr #1
	lsr	w14, w14, #2
	cmp	w14, #9
	b.hi	LBB0_4
; %bb.8:                                ;   in Loop: Header=BB0_5 Depth=1
	adr	x15, LBB0_9
	ldrb	w16, [x12, x14]
	add	x15, x15, x16, lsl #2
	br	x15
LBB0_9:                                 ;   in Loop: Header=BB0_5 Depth=1
	strb	w13, [x0], #1
	b	LBB0_3
LBB0_10:                                ;   in Loop: Header=BB0_5 Depth=1
	orr	w14, w13, #0x80
	strb	w14, [x0]
	lsr	x13, x13, #7
	strb	w13, [x0, #1]
	ldr	x13, [x1]
	add	x13, x13, #2
	str	x13, [x1]
	add	x0, x0, #2
	b	LBB0_4
LBB0_11:                                ;   in Loop: Header=BB0_5 Depth=1
	orr	w14, w13, #0x80
	strb	w14, [x0]
	ubfx	x14, x13, #7, #25
	orr	w14, w14, #0x80
	strb	w14, [x0, #1]
	lsr	x13, x13, #14
	strb	w13, [x0, #2]
	ldr	x13, [x1]
	add	x13, x13, #3
	str	x13, [x1]
	add	x0, x0, #3
	b	LBB0_4
LBB0_12:                                ;   in Loop: Header=BB0_5 Depth=1
	orr	w14, w13, #0x80
	strb	w14, [x0]
	ubfx	x14, x13, #7, #25
	orr	w14, w14, #0x80
	strb	w14, [x0, #1]
	ubfx	x14, x13, #14, #18
	orr	w14, w14, #0x80
	strb	w14, [x0, #2]
	lsr	x13, x13, #21
	strb	w13, [x0, #3]
	ldr	x13, [x1]
	add	x13, x13, #4
	str	x13, [x1]
	add	x0, x0, #4
	b	LBB0_4
LBB0_13:                                ;   in Loop: Header=BB0_5 Depth=1
	orr	w14, w13, #0x80
	strb	w14, [x0]
	ubfx	x14, x13, #7, #25
	orr	w14, w14, #0x80
	strb	w14, [x0, #1]
	ubfx	x14, x13, #14, #18
	orr	w14, w14, #0x80
	strb	w14, [x0, #2]
	ubfx	x14, x13, #21, #11
	orr	w14, w14, #0x80
	strb	w14, [x0, #3]
	lsr	x13, x13, #28
	strb	w13, [x0, #4]
	ldr	x13, [x1]
	add	x13, x13, #5
	str	x13, [x1]
	add	x0, x0, #5
	b	LBB0_4
LBB0_14:                                ;   in Loop: Header=BB0_5 Depth=1
	orr	w14, w13, #0x80
	strb	w14, [x0]
	ubfx	x14, x13, #7, #25
	orr	w14, w14, #0x80
	strb	w14, [x0, #1]
	ubfx	x14, x13, #14, #18
	orr	w14, w14, #0x80
	strb	w14, [x0, #2]
	ubfx	x14, x13, #21, #11
	orr	w14, w14, #0x80
	strb	w14, [x0, #3]
	lsr	x14, x13, #28
	orr	w14, w14, #0x80
	strb	w14, [x0, #4]
	lsr	x13, x13, #35
	strb	w13, [x0, #5]
	ldr	x13, [x1]
	add	x13, x13, #6
	str	x13, [x1]
	add	x0, x0, #6
	b	LBB0_4
LBB0_15:                                ;   in Loop: Header=BB0_5 Depth=1
	orr	w14, w13, #0x80
	strb	w14, [x0]
	ubfx	x14, x13, #7, #25
	orr	w14, w14, #0x80
	strb	w14, [x0, #1]
	ubfx	x14, x13, #14, #18
	orr	w14, w14, #0x80
	strb	w14, [x0, #2]
	ubfx	x14, x13, #21, #11
	orr	w14, w14, #0x80
	strb	w14, [x0, #3]
	lsr	x14, x13, #28
	orr	w14, w14, #0x80
	strb	w14, [x0, #4]
	lsr	x14, x13, #35
	orr	w14, w14, #0x80
	strb	w14, [x0, #5]
	lsr	x13, x13, #42
	strb	w13, [x0, #6]
	ldr	x13, [x1]
	add	x13, x13, #7
	str	x13, [x1]
	add	x0, x0, #7
	b	LBB0_4
LBB0_16:                                ;   in Loop: Header=BB0_5 Depth=1
	orr	w14, w13, #0x80
	strb	w14, [x0]
	ubfx	x14, x13, #7, #25
	orr	w14, w14, #0x80
	strb	w14, [x0, #1]
	ubfx	x14, x13, #14, #18
	orr	w14, w14, #0x80
	strb	w14, [x0, #2]
	ubfx	x14, x13, #21, #11
	orr	w14, w14, #0x80
	strb	w14, [x0, #3]
	lsr	x14, x13, #28
	orr	w14, w14, #0x80
	strb	w14, [x0, #4]
	lsr	x14, x13, #35
	orr	w14, w14, #0x80
	strb	w14, [x0, #5]
	lsr	x14, x13, #42
	orr	w14, w14, #0x80
	strb	w14, [x0, #6]
	lsr	x13, x13, #49
	strb	w13, [x0, #7]
	ldr	x13, [x1]
	add	x13, x13, #8
	str	x13, [x1]
	add	x0, x0, #8
	b	LBB0_4
LBB0_17:                                ;   in Loop: Header=BB0_5 Depth=1
	orr	w14, w13, #0x80
	strb	w14, [x0]
	ubfx	x14, x13, #7, #25
	orr	w14, w14, #0x80
	strb	w14, [x0, #1]
	ubfx	x14, x13, #14, #18
	orr	w14, w14, #0x80
	strb	w14, [x0, #2]
	ubfx	x14, x13, #21, #11
	orr	w14, w14, #0x80
	strb	w14, [x0, #3]
	lsr	x14, x13, #28
	orr	w14, w14, #0x80
	strb	w14, [x0, #4]
	lsr	x14, x13, #35
	orr	w14, w14, #0x80
	strb	w14, [x0, #5]
	lsr	x14, x13, #42
	orr	w14, w14, #0x80
	strb	w14, [x0, #6]
	lsr	x14, x13, #49
	orr	w14, w14, #0x80
	strb	w14, [x0, #7]
	lsr	x13, x13, #56
	strb	w13, [x0, #8]
	ldr	x13, [x1]
	add	x13, x13, #9
	str	x13, [x1]
	add	x0, x0, #9
	b	LBB0_4
LBB0_18:                                ;   in Loop: Header=BB0_5 Depth=1
	orr	w14, w13, #0x80
	strb	w14, [x0]
	dup.2d	v5, x13
	ushl.2d	v6, v5, v0
	ushl.2d	v7, v5, v1
	ushl.2d	v16, v5, v2
	ushl.2d	v5, v5, v3
	uzp1.4s	v5, v5, v16
	uzp1.4s	v6, v7, v6
	uzp1.8h	v5, v6, v5
	xtn.8b	v5, v5
	orr.8b	v5, v5, v4
	stur	d5, [x0, #1]
	lsr	x13, x13, #63
	strb	w13, [x0, #9]
	ldr	x13, [x1]
	add	x13, x13, #10
	str	x13, [x1]
	add	x0, x0, #10
	b	LBB0_4
LBB0_19:
	ret
	.loh AdrpAdd	Lloh8, Lloh9
	.loh AdrpAdrp	Lloh6, Lloh8
	.loh AdrpLdr	Lloh6, Lloh7
	.loh AdrpAdrp	Lloh4, Lloh6
	.loh AdrpLdr	Lloh4, Lloh5
	.loh AdrpLdr	Lloh2, Lloh3
	.loh AdrpAdrp	Lloh0, Lloh2
	.loh AdrpLdr	Lloh0, Lloh1
	.cfi_endproc
	.section	__TEXT,__const
lJTI0_0:
	.byte	(LBB0_9-LBB0_9)>>2
	.byte	(LBB0_10-LBB0_9)>>2
	.byte	(LBB0_11-LBB0_9)>>2
	.byte	(LBB0_12-LBB0_9)>>2
	.byte	(LBB0_13-LBB0_9)>>2
	.byte	(LBB0_14-LBB0_9)>>2
	.byte	(LBB0_15-LBB0_9)>>2
	.byte	(LBB0_16-LBB0_9)>>2
	.byte	(LBB0_17-LBB0_9)>>2
	.byte	(LBB0_18-LBB0_9)>>2
                                        ; -- End function
.subsections_via_symbols
