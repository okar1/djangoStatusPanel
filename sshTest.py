#!/usr/bin/python3.5
# -*- coding: utf-8 -*-

import subprocess

cmd1 = ['ssh', 'm7-ql-bkj01.node.qos',
       'cat - > /home/qladmin/file1.dat']

cmd2 = ['ssh', 'm7-ql-bkj01.node.qos',
       'cat /home/qladmin/file1.dat']

cmd3 = ['ssh', 'm7-ql-bkj01.node.qos',
       'mv /home/qladmin/file1.dat /home/qladmin/file2.dat']

cmd4 = ['ssh', 'm7-ql-bkj01.node.qos',
       'ls /home/qladmin/file1.dat']

s="""preved на русском
kak
dela"""


def write():
	with subprocess.Popen(cmd1, stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=subprocess.PIPE) as p:
		p.stdin.write(s.encode())
		# p.wait()    # дождаться выполнени
		res = p.communicate()  # получить tuple('stdout', 'stderr')
		
		print ( res)
		if p.returncode:
			print("error:",res[1])
			
		else:
			print("OK")


def read():
	with subprocess.Popen(cmd2, stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=subprocess.PIPE) as p:
		# for line in p.stdin:
		# 	print(line)
		res = p.communicate()  # получить tuple('stdout', 'stderr')
		
		print ( res)
		if p.returncode:
			print("error:",res[1])
			
		else:
			print (res[0].decode())
			print("OK")

def rename():
	with subprocess.Popen(cmd3, stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=subprocess.PIPE) as p:
		# for line in p.stdin:
		# 	print(line)
		res = p.communicate()  # получить tuple('stdout', 'stderr')
		
		print ( res)
		if p.returncode:
			print("error:",res[1].decode())
			
		else:
			print ("OK",res[0].decode())

def exists():
	with subprocess.Popen(cmd4, stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=subprocess.PIPE) as p:
		# for line in p.stdin:
		# 	print(line)
		res = p.communicate()  # получить tuple('stdout', 'stderr')
		
		print ( res)
		if p.returncode:
			print("error:",res[1].decode())
			
		else:
			print ("OK",res[0].decode())


exists()