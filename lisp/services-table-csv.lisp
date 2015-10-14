#!/usr/bin/sbcl --script

(setq *print-pretty* 'nil)

(if (sb-ext:posix-getenv "QUERY_STRING")
	(setq customer (subseq (string (sb-ext:posix-getenv "QUERY_STRING")) 5))
	(setq customer "ALL"))
;(setq customer (sb-ext:posix-getenv "QUERY_STRING"))
(setq VERSION "0.5cgi")


(let ((quicklisp-init (merge-pathnames "/home/alex/quicklisp/setup.lisp" (user-homedir-pathname))))
  (when (probe-file quicklisp-init)
    (load quicklisp-init)))

(require :cl-mysql)

(defun filename ()
  (if (equal common-lisp-user::customer "ALL")
		'"SERVICES.csv"
		(format nil "SERVICES-~A.csv" common-lisp-user::customer)))

(defun sql ()
    (cl-mysql:connect
		:host     "10.52.1.6"
		:database "centreon"
		:user     "ro_user"
		:password "FLQPFKcy7qC4YxFL")

    (if (equal common-lisp-user::customer "ALL")
        (setq list_s (car (car (cl-mysql:query "SELECT service_id, service_description, service_comment, command_command_id_arg, service_activate FROM service"))))
        (setq list_s (car (car (cl-mysql:query
            (format nil "SELECT service_id, service_description, service_comment, command_command_id_arg, service_activate FROM service WHERE service_description REGEXP \"~A-\"" common-lisp-user::customer))))))

    (setq list_h (car (car (cl-mysql:query "SELECT host_id, host_name, host_alias, host_address, host_activate FROM host ORDER BY host_name"))))
    (setq list_g (car (car (cl-mysql:query "SELECT cg_id, cg_name from contactgroup"))))
    (setq list_p (car (car (cl-mysql:query "SELECT id, name, ns_ip_address from nagios_server"))))
    (setq list_c (car (car (cl-mysql:query "SELECT contact_id, contact_name FROM contact"))))

    (setq relation_h_p (car (car (cl-mysql:query "SELECT host_host_id, nagios_server_id  FROM ns_host_relation"))))
    (setq relation_s_c (car (car (cl-mysql:query "SELECT service_service_id, contact_id FROM contact_service_relation"))))
    (setq relation_s_g (car (car (cl-mysql:query "SELECT service_service_id, contactgroup_cg_id FROM contactgroup_service_relation"))))
    (setq relation_s_h (car (car (cl-mysql:query "SELECT service_service_id, host_host_id FROM host_service_relation")))))

(defun state (x)
    (if (equal x 1)
		'"Enabled"
		'"Disabled"))

(defun contact-hash-relation (hash-relation list-relation)
  (dolist (i list-relation)
    (if (gethash (nth 0 i) hash-relation)
      (progn
        (if (consp (gethash (nth 0 i) hash-relation))
          (setf a (gethash (nth 0 i) hash-relation))
          (setf a (list (gethash (nth 0 i) hash-relation))))
        (setf (gethash (nth 0 i) hash-relation) (append a (list (nth 1 i)))))
    (setf (gethash (nth 0 i) hash-relation) (nth 1 i)))))


(defun data ()
    ; service - host
    (setf hash-relation-s-h (make-hash-table))
    (dolist (i relation_s_h)
        (setf (gethash (nth 0 i) hash-relation-s-h) (nth 1 i)))

    (setf hash-host (make-hash-table))
    (dolist (i list_h)
        (setf (gethash (car i) hash-host) (cdr i)))

    ; service - contact-groups
    (setf hash-relation-s-g (make-hash-table))
    (contact-hash-relation hash-relation-s-g relation_s_g)

    (setf hash-group (make-hash-table))
    (dolist (i list_g)
        (setf (gethash (car i) hash-group) (cdr i)))

    ; host - poller
    (setf hash-relation-h-p (make-hash-table))
    (dolist (i relation_h_p)
        (setf (gethash (nth 0 i) hash-relation-h-p) (nth 1 i)))

    (setf hash-poller (make-hash-table))
    (dolist (i list_p)
        (setf (gethash (car i) hash-poller) (cdr i)))

    ; service - contacts
    (setf hash-relation-s-c (make-hash-table))
    (contact-hash-relation hash-relation-s-c relation_s_c)

    (setf hash-contact (make-hash-table))
    (dolist (i list_c)
        (setf (gethash (car i) hash-contact) (cdr i))))


(defun contact-string (list-contact hash)
	(setf msg (car (list 'nil)))
    (dolist (x list-contact)
		(setf msg (append msg (list '|, |) (gethash x hash))))
	(cdr msg))


(defun make-str (i)
	(let(
	(s-id		(nth 0 i))
	(s-name 	(nth 1 i))
	(h-name 	(nth 0 (gethash (gethash (car i) hash-relation-s-h) hash-host)))
	(h-alias	(nth 1 (gethash (gethash (car i) hash-relation-s-h) hash-host)))
	(h-ip		(nth 2 (gethash (gethash (car i) hash-relation-s-h) hash-host)))
	(h-activate
		(if (null (nth 3 (gethash (gethash (car i) hash-relation-s-h) hash-host)))
			'|ERROR|
			(state (parse-integer (nth 3 (gethash (gethash (car i) hash-relation-s-h) hash-host))))))

	(s-activate	(state (parse-integer (nth 4 i))))

	(contacts 
		(if (null (gethash (car i) hash-relation-s-c))
			'|< NO ALERTING >|
			(if (consp (gethash (car i) hash-relation-s-c))
				(contact-string (gethash (car i) hash-relation-s-c) hash-contact)
				(if (numberp (gethash (car i) hash-relation-s-c))
					(nth 0 (gethash (gethash (car i) hash-relation-s-c) hash-contact))
					'|< NO ALERTING >|))))
	(contact-group
		(if (null (gethash (car i) hash-relation-s-g))
			'|< NO ALERTING >| 
			(if (consp (gethash (car i) hash-relation-s-g))
				(contact-string (gethash (car i) hash-relation-s-g) hash-group)
				(if (numberp (gethash (car i) hash-relation-s-g))
					(nth 0 (gethash (gethash (car i) hash-relation-s-g) hash-group))
					'|< NO ALERTING >|))))

	(p-name     (nth 0 (gethash (gethash (gethash (car i) hash-relation-s-h) hash-relation-h-p) hash-poller)))
	(p-ip       (nth 1 (gethash (gethash (gethash (car i) hash-relation-s-h) hash-relation-h-p) hash-poller)))
	(s-values   (nth 3 i)))
	;(comments   (nth 2 i)))

	(return-from make-str
		(list s-id s-name h-name h-alias h-ip h-activate s-activate contacts contact-group p-name p-ip s-values))))
			
(defun make-table (&optional file)
	(setf L (list '()))
	(dolist (i list_s)
		(nconc L (list (make-str i))))

	(setf L (sort (cdr L) #'string< :key #'third))
	(push '("SERVICE ID#SERVICE NAME#HOST NAME#HOST ALIAS#IP ADDRESS#HOST ACTIVATE#SERVICE ACTIVATE#CONTACTS#CONTACT GROUPS#POLLER NAME#POLLER IP#SERVICE VALUES#COMMENTS") L)

	(format t "Content-Type: application/vnd.ms-excel~%")
	(format t "Content-Disposition: attachment; filename=~A~%~%" file)
    (dolist (i L)
        (dolist (row i)
			(if (consp row)
                (progn
                    (dolist (r row)
                        (format t "~A" r))
                    (format t "#"))
                (format t "~A#" row)))
        (format t "~%")))
	;(format t "~A~%" (length list_s))

	;(dolist (i L)
	;	(format t "~A~%" i)))

(sql)
(data)
(make-table (filename))
