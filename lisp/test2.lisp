#!/usr/bin/sbcl --script

;(format t "Content-type: text/html ~C~C~C~C" #\return #\linefeed #\return #\linefeed)

(let ((quicklisp-init (merge-pathnames "/home/alex/quicklisp/setup.lisp" (user-homedir-pathname))))
  (when (probe-file quicklisp-init)
    (load quicklisp-init)))

;(require :asdf)

;(require :cl-csv)
(with-open-file (*standard-output* "/dev/null" :direction :output
                                   :if-exists :supersede)
				(require :buildnode-excel))

;(in-package :buildnode-excel)

(defun make-table (&optional file)
	;(in-package :buildnode-excel)
  (let ((doc (excel:with-excel-workbook ()

  (ss:worksheet `("ss:Name" "test")
    (ss:table ()

    (excel:set-index 5
      (ss:row ()
        (excel:set-index 0
          (ss:header-cell "Test"))
          (ss:header-cell "OK")
          (ss:header-cell "Yahoooo")))
          )))))


;(with-open-file (*error-output* "/dev/null" :direction :output
;                                   :if-exists :supersede)
;			(buildnode-excel::write-doc-to-file doc file))

;  	(values (buildnode-excel::document-to-string doc) doc)


(format t "Content-Type:application/vnd.ms-excel~%")
(format t "Content-Disposition:attachment;filename=~A~%~%" file)
;(princ (buildnode-excel::write-doc-to-file doc t))


(buildnode-excel::write-doc-to-file doc *standard-output*))
;(values (buildnode-excel::document-to-string doc) doc)
;(format t "~A" doc))
;(princ "Test. Ok!"))
)

(make-table "aaa.xls")

;(format t "Content-Type:application/vnd.ms-excel~%")
;(format t "Content-Disposition:attachment;filename=file.xls~%~%")
;(princ "Test. Ok!")

