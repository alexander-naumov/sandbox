#!/usr/bin/sbcl --script

(let ((quicklisp-init (merge-pathnames "/home/alex/quicklisp/setup.lisp" (user-homedir-pathname))))
  (when (probe-file quicklisp-init)
(load quicklisp-init)))

(require :snmp)

(defun get-info (hash)
	(format t "~A~%"
		(snmp:with-open-session
  		(s (gethash "HOST" hash)
				:user (gethash "USERNAME" hash)
				:auth (list ':md5 (gethash "PASSWORD" hash)))
			(snmp:snmp-walk s (gethash "OPTION" hash)))))


(defun cmd (param)
	(setf hash (make-hash-table :test 'equal))
	(dolist (p param)
		(if (member (concatenate 'string "-" (subseq p 0 1)) sb-ext:*posix-argv* :test #'equal)
			(setf (gethash p hash)
				(nth (+ (position (concatenate 'string "-" (subseq p 0 1)) sb-ext:*posix-argv* :test #'equal) 1) sb-ext:*posix-argv*))
			(format t "ERROR: no ~A parameter set" (concatenate 'string "-" (subseq p 0 1)))))
	hash)


(get-info (cmd (list "HOST" "USERNAME" "PASSWORD" "OPTION")))

