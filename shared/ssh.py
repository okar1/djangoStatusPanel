# -*- coding: utf-8 -*-
import subprocess

class RemoteHost:
	
	host=""
	
	def __init__(self,host):
		self.host=host

	def readFile(self,fileName):
		cmd = ['ssh', self.host, 'cat '+fileName]
		with subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=subprocess.PIPE) as p:
			r = p.communicate()  # получить tuple('stdout', 'stderr')
			if p.returncode:
				error=r[1].decode()
				result=None
			else:
				error=None
				result=r[0].decode()
		return (result,error)


	def writeFile(self,fileName,text):

		# return None

		cmd = ['ssh', self.host, 'cat - > '+fileName]
		with subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=subprocess.PIPE) as p:
			p.stdin.write(text.encode())
			r = p.communicate()  # получить tuple('stdout', 'stderr')
			if p.returncode:
				error=r[1].decode()
			else:
				error=None
		return error

	def moveFile(self,oldFileName,newFileName):

		# return None

		cmd = ['ssh', self.host, 'mv '+oldFileName+' '+newFileName]
		with subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=subprocess.PIPE) as p:
			r = p.communicate()  # получить tuple('stdout', 'stderr')
			if p.returncode:
				error=r[1].decode()
			else:
				error=None
		return error

	def fileExists(self,fileName):
		cmd = ['ssh', self.host, 'ls '+fileName]
		with subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=subprocess.PIPE) as p:
			r = p.communicate()  # получить tuple('stdout', 'stderr')
			if p.returncode:
				if p.returncode==2:  #define ENOENT 2 /* No such file or directory */
					result=False
					error=None
				else:
					result=None
					error=r[1].decode()
			else:
				result=True
				error=None
		return (result,error)

	def restartDaemon(self,daemonName):

		# return None

		cmd = ['ssh', self.host, 'sudo', 'bash', '-c', '"systemctl restart '+daemonName+'"' ]
		with subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=subprocess.PIPE) as p:
			r = p.communicate()  # получить tuple('stdout', 'stderr')
			if p.returncode:
				error=r[1].decode()
			else:
				error=None
		return error

	def allowAccess(self,user,group,path):
		cmd="chown -R {user}:{group} {path} && chmod -R u+rw,g+rw {path}".format(user=user,group=group,path=path)
		cmd = ['ssh', self.host, 'sudo', 'bash', '-c', '"'+cmd+'"' ]
		with subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=subprocess.PIPE) as p:
			r = p.communicate()  # получить tuple('stdout', 'stderr')
			if p.returncode:
				error=r[1].decode()
			else:
				error=None
		return error




