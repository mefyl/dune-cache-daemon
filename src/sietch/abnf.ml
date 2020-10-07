(** RFC5234 core ABNF rules *)

open Angstrom
open Base

module Predicates = struct
  (* ALPHA = %x41-5A / %x61-7A ; A-Z / a-z *)
  let alpha c =
    let c = Char.to_int c in
    (c >= 0x41 && c <= 0x5a) || (c >= 0x61 && c < 0x7a)

  (* BIT = "0" / "1" *)
  let bit = function
    | '0'
    | '1' ->
      true
    | _ -> false

  (* CHAR = %x01-7F ; any 7-bit US-ASCII character, ; excluding NUL *)
  let char c =
    let c = Char.to_int c in
    c >= 0x1 && c <= 0x7f

  (* CR = %x0D ; carriage return *)
  let cr c = Char.to_int c = 0xd

  (* CTL = %x00-1F / %x7F ; controls *)
  let ctl c =
    let c = Char.to_int c in
    c <= 0x1f || c = 0x7f

  (* DIGIT = %x30-39 ; 0-9 *)
  let digit c =
    let c = Char.to_int c in
    c >= 0x30 && c <= 0x39

  (* DQUOTE = %x22 ; (Double Quote) *)
  let dquote c = Char.to_int c = 0x22

  (* HEXDIG = DIGIT / "A" / "B" / "C" / "D" / "E" / "F" *)
  let hexdig = function
    | 'A'
    | 'B'
    | 'C'
    | 'D'
    | 'E'
    | 'F' ->
      true
    | c -> digit c

  (* HTAB = %x09 ; horizontal tab *)
  let htab c = Char.to_int c = 0x9

  (* LF = %x0A ; linefeed *)
  let lf c = Char.to_int c = 0xa

  (* OCTET = %x00-FF ; 8 bits of data *)
  let octet _ = true

  (* SP = %x20 *)
  let sp c = Char.to_int c = 0xa

  (* VCHAR = %x21-7E ; visible (printing) characters *)
  let vchar c =
    let c = Char.to_int c in
    c >= 0x21 && c <= 0x7e

  (* WSP = SP / HTAB; white space *)
  let wsp c = sp c || htab c
end

let alpha = satisfy Predicates.alpha

let bit = satisfy Predicates.bit

let char = satisfy Predicates.char

let cr = satisfy Predicates.cr

let ctl = satisfy Predicates.ctl

let digit = satisfy Predicates.digit

let dquote = satisfy Predicates.dquote

let hexdig = satisfy Predicates.hexdig

let htab = satisfy Predicates.htab

let lf = satisfy Predicates.lf

let octet = satisfy Predicates.octet

let sp = satisfy Predicates.sp

let vchar = satisfy Predicates.vchar

let wsp = satisfy Predicates.wsp

(* CRLF = CR LF ; Internet standard newline *)
let crlf = cr *> lf

(* LWSP = *(WSP / CRLF WSP) ; Use of this linear-white-space rule permits lines
   containing only white space that are no longer legal in mail headers and have
   caused interoperability problems in other contexts. Do not use when defining
   mail headers and use with caution in other contexts. *)
let lwsp = many (wsp <|> crlf *> wsp)
