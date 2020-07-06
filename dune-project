(lang dune 2.1)

(name dune-cache-daemon)

(version 0.1.0)

(generate_opam_files true)

(license MIT)

(maintainers "Jane Street Group, LLC <opensource@janestreet.com>")

(authors "Jane Street Group, LLC <opensource@janestreet.com>")

(source
 (github ocaml/dune-cache-daemon))

(documentation "https://dune.readthedocs.io/")

(package
 (name dune-cache-daemon)
 (synopsis "Dune distributed cache daemon")
 (depends
  (dune-private-libs
   (>= 2.4.0))
  (csexp
   (= 1.2.3))
  cmdliner
  git-unix
  httpaf
  httpaf-lwt-unix
  irmin
  irmin-git
  irmin-mem
  logs
  lwt
  lwt_ppx))
