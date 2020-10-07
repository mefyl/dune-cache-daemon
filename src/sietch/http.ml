open Abnf
open Angstrom
open Base

module Fn = struct
  include Fn

  let ( || ) f g x = f x || g x
end

(* Comments sections refer to RFC 7230. *)

(* 3.2.6. Field Value Components *)

(* tchar = "!" / "#" / "$" / "%" / "&" / "'" / "*" / "+" / "-" / "." / "^" / "_"
   / "`" / "|" / "~" / DIGIT / ALPHA ; any VCHAR, except delimiters *)

let tchar = function
  | '!'
  | '#'
  | '$'
  | '%'
  | '&'
  | '\''
  | '*'
  | '+'
  | '-'
  | '.'
  | '^'
  | '_'
  | '`'
  | '|'
  | '~' ->
    true
  | c -> Predicates.digit c || Predicates.alpha c

(* token = 1*tchar *)

let token = take_while1 tchar

(* obs-text = %x80-FF*)
let obs_text c =
  let c = Char.to_int c in
  c >= 0x80

(* qdtext = HTAB / SP /%x21 / %x23-5B / %x5D-7E / obs-text *)
let qdtext =
  let others c =
    let c = Char.to_int c in
    c = 0x21 || (c >= 0x23 && c <= 0x5b) || (c >= 0x5d && c <= 0x7e)
  in
  satisfy Fn.(Predicates.htab || Predicates.sp || obs_text || others)

(* quoted-pair = "\\" ( HTAB / SP / VCHAR / obs-text ) *)
let quoted_pair =
  char '\\'
  *> satisfy
       Fn.(Predicates.htab || Predicates.sp || Predicates.vchar || obs_text)

(* quoted-string = DQUOTE *( qdtext / quoted-pair ) DQUOTE *)
let quoted_string = dquote *> many (qdtext <|> quoted_pair) <* dquote

(* ctext = HTAB / SP / %x21-27 / %x2A-5B / %x5D-7E / obs-text*)
let ctext =
  let others c =
    let c = Char.to_int c in
    (c >= 0x21 && c <= 0x27)
    || (c >= 0x2a && c <= 0x5b)
    || (c >= 0x5d && c <= 0x7e)
  in
  satisfy Fn.(Predicates.htab || Predicates.sp || obs_text || others)

(* comment = "(" *( ctext / quoted-pair / comment ) ")" *)
(* FIXME: recurse *)
let comment = char '(' *> many1 (ctext <|> quoted_pair) <* char ')'

(* 5.3.1. origin-form *)

(* origin-form = absolute-path [ "?" query ] *)

let origin_form = absolute_path *> option (char '?' *> query)

(* 5.3.2. absolute-form *)

(* absolute-form = absolute-URI *)

let absolute_form = absolute_uri

(* 5.3.3. authority-form *)

(* authority-form = authority *)

let authority_form = authority

(* 5.3.4. asterisk-form *)

(* asterisk-form = "*" *)
let asterisk_form = char '*'

(* 5.2 Request target *)

(* request-target = origin-form / absolute-form / authority-form / asterisk-form *)
let request_target =
  origin_form <|> absolute_form <|> authority_form <|> asterisk_form
  <?> "request_target"

(* 3.1.1. Request Line *)

(* method = token *)

let method_ = token

(* request-line = method SP request-target SP HTTP-version CRLF *)

let request_line = method_ *> sp *> request_target *> sp *> http_version *> crlf

(* 3.1.2. Status Line *)

(* status-code = 3DIGIT *)
let status_code = digit *> digit *> digit

(* reason-phrase = *( HTAB / SP / VCHAR / obs-text ) *)
let reason_phrase =
  take_while (fun c ->
      Predicates.htab c || Predicates.sp c || Predicates.vchar c || obs_text c)

(* status-line = HTTP-version SP status-code SP reason-phrase CRLF *)
let status_line =
  http_version *> sp *> status_code *> sp *> reason_phrase *> crlf

(* 3.1 Start Line *)

(* start-line = request-line / status-line *)

let start_line = request_line <|> status_line

(* 3.2.3. Whitespace *)

(* OWS = *( SP / HTAB ) ; optional whitespace *)

let ows = many (satisfy (fun c -> Predicates.sp c || Predicates.htab c))

(* RWS = 1*( SP / HTAB ) ; required whitespace *)

let ows = many1 (satisfy (fun c -> Predicates.sp c || Predicates.htab c))

(* BWS = OWS ; "bad" whitespace *)

let bws = ows

(* 3.2. Header Fields *)

(* field-name = token *)
let field_name = token

(* field-vchar = VCHAR / obs-text *)
let field_vchar = satisfy (fun c -> Predicates.vchar c || obs_text c)

(* field-content = field-vchar [ 1*( SP / HTAB ) field-vchar ] *)
let field_content =
  field_vchar
  *> option
       ( take_while1 (fun c -> Predicates.sp c || Predicates.htab c)
       *> field_vchar )

(* field-value = *( field-content / obs-fold ) *)
let field_value = many (field_content <|> obs_fold)

(* header-field = field-name ":" OWS field-value OWS *)
let header_field = field_name *> char ':' *> ows *> field_value *> ows

(* obs-fold = CRLF 1*( SP / HTAB ) ; obsolete line folding see Section 3.2.4 *)

(* 3. Message Format *)

(* HTTP-message = start-line *( header-field CRLF ) CRLF [ message-body ] *)

let http_message =
  start_line *> many (header_field <* crlf) *> crlf *> option message_body
