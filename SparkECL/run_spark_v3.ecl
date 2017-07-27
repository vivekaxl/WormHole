/**
	This script would pass a dataset to the embedded 
	python function and return a dataset.
*/

IMPORT Python;
rs := {integer id, integer val};
ds1 := dataset([{1,1}], rs);

rs trans1(integer id, rs l) := TRANSFORM
	SELF.id := id;
	SELF.val := random();
END;
ds2 := normalize(ds1, 10, trans1(COUNTER, LEFT));
output(ds2);

ret_rs := {integer id, SET of string type_of};
dataset(ret_rs) run_command(dataset(rs) recs) := EMBED(python)
	for i,rec in enumerate(recs):
		yield (i,  dir(rec))
ENDEMBED;

output(run_command(ds2));