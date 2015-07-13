#!/usr/bin/clisp

(load "~/quicklisp/setup.lisp")
(ql:quickload "usocket")

(defun scan (host ports)
	(loop for p in ports
		do (ignore-errors
			(format t "~%port ~A: " p)
			(let* ((socket (usocket:socket-connect host (parse-integer p)))
				(stream (usocket:socket-stream socket)))
				(let ((result (read-line stream)))
					(close stream)
					(usocket:socket-close socket)
					(format t "~A~%" result))))))

(scan (car *args*) (cdr *args*))
