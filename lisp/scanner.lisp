#!/usr/bin/clisp

(load "~/quicklisp/setup.lisp")  
(ql:quickload "usocket")

(defun scan (ports)
    "show open ports"
        (loop for p in ports
            do (ignore-errors
                (let* ((socket (usocket:socket-connect "openbsd.org" p))
                    (stream (usocket:socket-stream socket)))
                        (let ((result (read-line stream)))
                            (close stream)
                                (usocket:socket-close socket)
                                    (format t "~%~d" result))))))
 
 (scan (list 20 21 22))
