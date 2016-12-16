#!/usr/bin/sbcl --script


(declaim (sb-ext:muffle-conditions cl:warning))

(let ((quicklisp-init (merge-pathnames "/home/alex/quicklisp/setup.lisp" (user-homedir-pathname))))
  (when (probe-file quicklisp-init)
(load quicklisp-init)))

(let ((*standard-output* (make-broadcast-stream)))
  (require :snmp))


(defun get-info (hash)
	(format t "~A~%" (car (cdr (car
		(snmp:with-open-session
  		(s (gethash "HOST" hash)
				:user (gethash "USERNAME" hash)
				:auth (list ':md5 (gethash "PASSWORD" hash)))
			(snmp:snmp-walk s "sysDescr")))))))

; FIXME
(defun cmd (param)
	(if (< (length sb-ext:*posix-argv*) 9)
		(progn
			(format t "Wrong number of elements~&")
			(sb-ext:exit 1))) ; FIXME

	(setf hash (make-hash-table :test 'equal))
	(dolist (p param)
		(if (member (concatenate 'string "-" (subseq p 0 1)) sb-ext:*posix-argv* :test #'equal)
			(setf (gethash p hash)
				(nth (+ (position (concatenate 'string "-" (subseq p 0 1)) sb-ext:*posix-argv* :test #'equal) 1) sb-ext:*posix-argv*))
			(format t "ERROR: no ~A parameter set" (concatenate 'string "-" (subseq p 0 1)))))
	hash)


(defun main ()
	(setf hash1 (cmd (list "HOST" "USERNAME" "PASSWORD" "OPTION")))

	(if (equal (gethash "OPTION" hash) "os")
		(get-info hash))
	(if (equal (gethash "OPTION" hash) "help")
		(help)))

(main)
