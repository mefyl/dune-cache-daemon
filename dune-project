(lang dune 2.1)
(name sietch)

(generate_opam_files true)

(license MIT)
(maintainers "Jane Street Group, LLC <opensource@janestreet.com>")
(authors "Jane Street Group, LLC <opensource@janestreet.com>")
(source (github ocaml/sietch))
(documentation "https://dune.readthedocs.io/")

(package
 (name sietch)
 ; The "depends" and "build" field are written in dune.opam.template
 (depends
  (dune-private-libs (= 2.4.0))
  (logs (>= 0.7.0))))
