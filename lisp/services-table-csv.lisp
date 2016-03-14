#!/usr/bin/sbcl --script

; Alexander Naumov <alexander_naumov@opensuse.org> 2015, 2016
;
; This CGI script generates service table. It's possible to generate
; table for concrete customer or table with all services.
; Data comes from the centreon DB.

(setq *print-pretty* 'nil) ;to get long (more than 80 characters) lines (needed by CSV)

(if (sb-ext:posix-getenv "QUERY_STRING")
	(setq customer
		(if (equal (subseq (string (sb-ext:posix-getenv "QUERY_STRING")) 5) "LEG")
			'LEGE
			(subseq (string (sb-ext:posix-getenv "QUERY_STRING")) 5)))
	(setq customer "ALL"))

(setq VERSION "0.8cgi")

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
		:host     ""
		:database ""
		:user     ""
		:password "")

	(setq list_s (car (car (cl-mysql:query "SELECT service_id, service_description, service_comment, command_command_id_arg, service_activate,
		service_template_model_stm_id, service_normal_check_interval, service_retry_check_interval, service_max_check_attempts FROM service"))))

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


(defun scheduling-req (x)
	(if (and (nth 1 x) (nth 2 x) (nth 3 x))
		(format nil "~A min / ~A min  (~A times)" (nth 1 x) (nth 2 x) (nth 3 x))
		(scheduling-req (gethash (car x) hash-relation-s-st))))


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

    ; service - scheduling
    (setf hash-relation-s-st (make-hash-table))
	(dolist (i list_s)
		(setf (gethash (car i) hash-relation-s-st) (list (nth 5 i) (nth 6 i) (nth 7 i) (nth 8 i))))

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


(defun make-str-service (i)
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

	(s-schedul  (scheduling-req (list (nth 5 i) (nth 6 i) (nth 7 i) (nth 8 i))))
	(p-name     (nth 0 (gethash (gethash (gethash (car i) hash-relation-s-h) hash-relation-h-p) hash-poller)))
	(p-ip       (nth 1 (gethash (gethash (gethash (car i) hash-relation-s-h) hash-relation-h-p) hash-poller)))
	(s-values   (nth 3 i)))

	(return-from make-str-service
		(list s-id s-name h-name h-alias h-ip h-activate s-activate s-schedul contacts contact-group p-name p-ip s-values))))

(defun make-str-host (i)
	(let(
	(h-name		(nth 1 i))
	(h-alias	(nth 2 i))
	(h-ip		(nth 3 i))
	(h-activate (state (parse-integer (nth 4 i)))))
	;(contacts 6)
	;(contact-group 8)
	;(p-name 7)
	;(p-ip 9)
	;(format t "~A ~A ~A ~A" h-name h-alias h-ip h-activate)
	(return-from make-str-host
		(list '--- '--- h-name h-alias h-ip h-activate '--- '--- '--- '---- '--- '---))))

(defun make-table (&optional file)
	(setf L (list '()))

;	(dolist (i list_s)
;		(if (string-equal common-lisp-user::customer (nth 2 (make-str-host i)) :start1 0 :end1 2 :start2 0 :end2 2)
;			(nconc L (list (make-str-service i)))))

	(if (string-equal common-lisp-user::customer "ALL")
		(progn
			(dolist (i list_h)
				(nconc L (list (make-str-host i))))
			(dolist (i list_s)
				(nconc L (list (make-str-service i)))))
		(progn
			(dolist (i list_h)									; FIXME: we call (make-str-host i) 2 times
				(if (string-equal common-lisp-user::customer (nth 2 (make-str-host i)) :start1 0 :end1 2 :start2 0 :end2 2)
					(nconc L (list (make-str-host i)))))
			(dolist (i list_s)
				(if (string-equal common-lisp-user::customer (nth 2 (make-str-service i)) :start1 0 :end1 2 :start2 0 :end2 2)
					(nconc L (list (make-str-service i)))))))

	(setf L (sort (cdr L) #'string< :key #'third))
	(push '("SERVICE ID#SERVICE NAME#HOST NAME#HOST ALIAS#IP ADDRESS#HOST ACTIVATE#SERVICE ACTIVATE#SCHEDULING#CONTACTS#CONTACT GROUPS#POLLER NAME#POLLER IP#SERVICE VALUES") L)

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

(sql)
(data)
(make-table (filename))
