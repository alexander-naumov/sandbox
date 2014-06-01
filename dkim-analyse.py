#!/usr/bin/env python

# Implementation for DKIM-signatur analyse (Domain Keys Identifer Mail)
# using OpenDKIM and Sendmail server. Policy filterings should be added
# to the database. Mail statistics will be generate and added to the 
# dkim_client_stat_tbl table.

import agn, re, sys, os, time, subprocess as sp
import datetime

agn.loglevel = agn.LV_DEBUG

ACTIVE = 'client_id IN (SELECT client_id FROM dkim_client_info_tbl WHERE status = \'active\')'

def ip_checker(c):
	list = []
	with open("/home/transact/bin/list_of_ips.txt", "w") as ip_file:
		for ip in c.query ('SELECT ip FROM dkim_client_ip_tbl WHERE %s' % ACTIVE):
			ip_file.write ('%s\n' % ip[0])
			list.append(ip[0])
	
	fd = open("/home/opendkim/etc/opendkim/TrustedHosts", "r")
        log = fd.read()
        fd.close()

        agn.log (agn.LV_DEBUG, 'ip_checker', 'list = %s' % list)
        agn.log (agn.LV_DEBUG, 'ip_checker', 'log = %s' % log)


	rest_log = filter(lambda x: x not in list, log.split("\n"))
	agn.log (agn.LV_DEBUG, 'ip_checker', 'log (after filter) = %s' % rest_log)

	if len(rest_log) > 1:
		agn.log (agn.LV_DEBUG, 'ip_checker', 'we should remove these IPs: %s' % rest_log[1:-1])
		if rest_log[1:-1] != []:
			agn.log (agn.LV_DEBUG, 'ip_checker', 'ok, good... now we are going to delete these IPs: %s' % rest_log[1:-1])


	list = filter(lambda x: x not in log.split("\n"), list)
        agn.log (agn.LV_DEBUG, 'ip_checker', 'list (after filter) = %s' % list)

	if len(list) > 0:
		agn.log (agn.LV_DEBUG, 'ip_checker', 'I am going to call add_ips()')
		agn.call (['add_ips'])

	if os.path.isfile("/home/transact/bin/list_of_ips.txt"):
		rc = agn.call ([agn.mkpath (agn.base, 'bin', 'smctrl'), 'access', 'load', '/home/transact/bin/list_of_ips.txt'])
	else:
		agn.log (agn.LV_DEBUG, 'ip_checker', 'File list_of_ips.txt is not there...')

	os.remove("/home/transact/bin/list_of_ips.txt")

def reconfig_dkim(c):
	domains_ = sp.Popen("domains_dkim", stdout=sp.PIPE)
	domains = domains_.communicate()[0]
	agn.log (agn.LV_DEBUG, 'reconfig_dkim', 'domains = \n%s' % domains)

	list = []
        
	DOM = c.queryc ('SELECT domain FROM dkim_client_key_tbl WHERE %s' % ACTIVE)
	for dmn in DOM:
		list.append(dmn[0])
		agn.log (agn.LV_DEBUG, 'reconfig_dkim', 'list = %s' % list)

	list = filter(lambda x: x not in domains, list)
	agn.log (agn.LV_DEBUG, 'reconfig_dkim', 'list (after filter) = %s' % list)

	for dmn in list:
		for key in c.queryc ('SELECT private_key, selector FROM dkim_client_key_tbl WHERE domain = :domain', {'domain': dmn}):
			agn.log (agn.LV_DEBUG, 'reconfig_dkim', 'I am going to call: reconf_dkim %s %s %s' % dmn, key[0], key[1])
			rc = agn.call ([agn.mkpath (agn.base, 'bin', 'reconf_dkim'), dmn, key[0], key[1]])


def create_logs():
	import log
	log.main()


def protocol(c, client_info, client_id):
	if re.search(">,<", client_info[2]):
		address = client_info[2].split(">,<")
		agn.log (agn.LV_DEBUG, 'protocol', 'address = %s' % address)

		for tbl_recipient in address:
			agn.log (agn.LV_DEBUG, 'protocol', 'tbl_recipient = %s' % tbl_recipient)
			client_info[2] = tbl_recipient

			x = c.execute ('INSERT INTO dkim_client_stat_tbl(client_id, recipient, status, time, sender, dkim) '
					'VALUES(:client_id, :recipient, :status, :time, :sender, :dkim)',
					{'client_id': client_id, 'recipient': client_info[2], 'status': client_info[3], 'time': client_info[1], 'sender': client_info[4], 'dkim': client_info[6]})
	else:	
		x = c.execute ('INSERT INTO dkim_client_stat_tbl(client_id, recipient, status, time, sender, dkim) '
				'VALUES(:client_id, :recipient, :status, :time, :sender, :dkim)',
				{'client_id': client_id, 'recipient': client_info[2], 'status': client_info[3], 'time': client_info[1], 'sender': client_info[4], 'dkim': client_info[6]})



def client_checker_domains(c, client_info, domains):
	if client_info[0] in domains:
                agn.log (agn.LV_DEBUG, 'client_checker_domains', 'I am going to call: protocol(%s, %s, %s)' % (c, client_info, domains[client_info[0]]))
		protocol (c, client_info, domains[client_info[0]])


def client_checker_ips(c, client_info, ip):
	for i in ip:
		if client_info[5] in i[1]:
			agn.log (agn.LV_DEBUG, 'client_checker_ips', 'I am going to call: protocol(%s, %s, %s)' % (c, client_info, i[0]))
        		protocol(c, client_info, i[0])


def parsing(log):
	agn.log (agn.LV_DEBUG, 'parsing', '-------- MAIL --------')

	if log[4] == ' ':
		day = log[5:6]
	else:
		day = log[4:6]

	time_ = log[7:15]

	month = {	"Jan":"01", "Feb":"02", "Mar":"03",
			"Apr":"04", "May":"05", "Jun":"06",
			"Jul":"07", "Aug":"08", "Sep":"09",
			"Oct":"10", "Nov":"11", "Dec":"12"}

	year = time.localtime().tm_year
	if month[log[:3]] == '12':
		if time.localtime().tm_mon == "1":
			year = int(year) - 1

        str_date = str(year) + " " + month[log[:3]] + " " + day + " " + time_
	tbl_date = datetime.datetime.fromtimestamp(time.mktime(time.strptime(str_date, "%Y %m %d %H:%M:%S")))

	tbl_dkim = "NO"
	tbl_domain = "None"
	tbl_ip = "None"
	tbl_from = "unknown"
	tbl_recipient = "unknown"
	tbl_stat = "unknown"

	for i in log[:-1].split('\n'):
        	i = i.split(' ')
               	a = 0
               	for y in i:
                      	a += 1
                       	if re.match("from=", y):
                               	tbl_from = y[6:-2]
                                agn.log (agn.LV_DEBUG, 'parsing', 'tbl_from = %s' % tbl_from)

                               	rest_n = 0
                               	for rest in i[a:]:
                                       	rest_n += 1
                                       	if re.match("relay=", rest):
						xx = i[-1].split("=")
						#print a[-1]
						#print "IP = ", xx[-1][1:-1]
                                               	#tbl_ip = i[a+rest_n][1:-1]
						tbl_ip = xx[-1][1:-1]
						agn.log (agn.LV_DEBUG, 'parsing', 'tbl_ip = %s' % tbl_ip)

			#if re.match("stat=", y):
                 	#	tbl_stat = " ".join(i[a-1:])
                        #       tbl_stat = tbl_stat[5:]
			#	agn.log (agn.LV_DEBUG, 'parsing', 'tbl_stat = %s' % tbl_stat)

                        if re.match("dsn=", y):
                                dsn = y[4:-1]
                                if re.match("2.", dsn):
                                        tbl_stat = "Sent"
                                if re.match("4.", dsn):
                                        tbl_stat = "Soft bounce"
                                if re.match("5.", dsn):
                                        tbl_stat = "Hard Bounce"
				agn.log (agn.LV_DEBUG, 'parsing', 'tbl_stat = %s' % tbl_stat)

			if re.match("d=", y):
                               	tbl_domain = y.split(';')
                               	tbl_domain = tbl_domain[0][2:]
				agn.log (agn.LV_DEBUG, 'parsing', 'tbl_domain = %s' % tbl_domain)
			
			if re.match("DKIM-Signature:", y):
				tbl_dkim = "DKIM-Signature"
				agn.log (agn.LV_DEBUG, 'parsing', 'tbl_dkim = %s' % tbl_dkim)

                        if re.match("to=", y):
                                tbl_recipient = y[4:-2]
				agn.log (agn.LV_DEBUG, 'parsing', 'tbl_recipient = %s' % tbl_recipient)


	client_info = [tbl_domain, tbl_date, tbl_recipient, tbl_stat, tbl_from, tbl_ip, tbl_dkim]
	return client_info


def main ():
	db = agn.DBaseID ('db.name')
	db.open ()
	c = db.cursor ()

	ip_checker(c)
	create_logs()
	reconfig_dkim(c)

	pwd = agn.mkpath (agn.base, 'var', 'spool', 'sendmail-log')
	list_of_logs = filter(lambda x: x.endswith('.log'), os.listdir(pwd))

	x_dmn = c.queryc ('SELECT client_id, domain FROM dkim_client_key_tbl WHERE %s' % ACTIVE)
	x_ip = c.queryc ('SELECT client_id, ip FROM dkim_client_ip_tbl WHERE %s' % ACTIVE)

	agn.log (agn.LV_DEBUG, 'main', 'x_dmn = %s' % x_dmn.data)
	agn.log (agn.LV_DEBUG, 'main', 'x_ip = %s' % x_ip.data)

	if list_of_logs:
		domains = {}
		for dmn in x_dmn:
			domains[dmn[1]] = dmn[0]
		count = 0

		for i in list_of_logs:
			path = agn.mkpath(pwd,i)
			fd = open(path, "r")
			log = fd.read()
			fd.close()
			client_info = parsing(log)

			if client_info[0] != "None":
				client_checker_domains(c, client_info, domains)
			else:
                                client_checker_ips(c, client_info, x_ip.data)
                        count += 1
                        if count % 1000 == 0:
                        	c.sync ()

		c.sync ()
		for i in list_of_logs:
			path = agn.mkpath(pwd,i)
			os.unlink (path)
	
	c.close ()
	db.close ()
	

if __name__=='__main__':
	while 1:
		main()
		time.sleep(300)
