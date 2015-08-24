#!/usr/bin/sbcl --script


(let ((quicklisp-init (merge-pathnames "/home/alex/quicklisp/setup.lisp" (user-homedir-pathname))))
  (when (probe-file quicklisp-init)
    (load quicklisp-init)))

(require :asdf)

;(require :cl-csv)
(with-open-file (*standard-output* "/dev/null" :direction :output
                                   :if-exists :supersede)
				(require :buildnode-excel))


(format t "Content-Type:application/vnd.ms-excel~%")
(format t "Content-Disposition:attachment;filename=file.xls~%~%")
(princ "Test. Ok!")

