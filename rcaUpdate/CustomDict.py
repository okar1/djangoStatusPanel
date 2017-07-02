import json

# https://docs.python.org/3/reference/datamodel.html?emulating-container-types#emulating-container-types
# custom dict class
# work like standard dict, but not inherits it
class CustomDict():
	def __init__(self):
		self.dict=dict()
	def __setitem__(self, key, item): 
		return self.dict.__setitem__(key, item)
	def __getitem__(self, key): 
		return self.dict.__getitem__(key)
	def __repr__(self): 
		return self.dict.__repr__()
	def __len__(self): 
		return self.dict.__len__()
	def __delitem__(self, key): 
		return self.dict.__delitem__(key)
	def __cmp__(self, dict):
		return self.dict.__cmp__(dict)
	def __contains__(self, item):
		return self.dict.__contains__(dict)
	def __iter__(self):
		return self.dict.__iter__()
	def __unicode__(self):
	    return self.dict.__unicode__()
	def __str__(self):
		return self.dict.__str__()
	def clear(self):
		return self.dict.clear()
	def copy(self):
		return self.dict.copy()
	def has_key(self, k):
		return self.dict.has_key(k)
	def pop(self, k, d=None):
		return self.dict.pop(k, d)
	def update(self, *args, **kwargs):
		return self.dict.update(*args, **kwargs)
	def keys(self):
		return self.dict.keys()
	def values(self):
		return self.dict.values()
	def items(self):
		return self.dict.items()

#custom json encoder. Returns CustomDict as list of values
#this works only if customdict not inherits dict 
class CustomEncoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, CustomDict):
			return list(obj.values())
		else:
			return super().encode(obj)


#********************************************
#********************************************
#********************************************
#example. Customdict encodes as list
if __name__ == "__main__":
	a=CustomDict()
	a['2']='3'
	a['22']='33'
	a[222]={2:'3'}
	b={}
	b['hello']=a
	print(json.dumps(b,cls=CustomEncoder,ensure_ascii=False, indent=True))
