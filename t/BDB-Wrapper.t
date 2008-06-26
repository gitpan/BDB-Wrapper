# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl BDB-Wrapper.t'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use Test;
BEGIN { plan tests => 22 };
use BDB::Wrapper;
use BerkeleyDB;
ok(1); # If we made it this far, we're ok.

#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.
my $bdbw;
ok($bdbw=new BDB::Wrapper);

my $bdb='test.bdb';
unlink $bdb if -f $bdb;
my $bdbh;
my $sort_code_ref=sub {lc $_[1] cmp lc $_[0]};
ok($bdbh=$bdbw->create_write_dbh($bdb, { 'reverse'=>1 }));

ok($bdbh->db_put(1, 1)==0);
my $test_value;
ok($bdbh->db_get(1, $test_value)==0);
ok($test_value==1);

ok($bdbh->db_put(2, 2)==0);
ok($bdbh->db_get(2, $test_value)==0);
ok($test_value==2);

ok($bdbh->db_put(3, 3)==0);
ok($bdbh->db_get(3, $test_value)==0);
ok($test_value==3);

ok($bdbh->db_put(4, 4)==0);
ok($bdbh->db_get(4, $test_value)==0);
ok($test_value==4);

my $key=0;
my $value;
my @values=();
if(my $cursor=$bdbh->db_cursor()){
  while($cursor->c_get($key, $value, DB_NEXT)==0){
	push(@values, $key);
  }
  $cursor->c_close();
}
ok($values[0]==4 && $values[1]==3 && $values[2]==2 && $values[3]==1);
ok($bdbh->db_close()==0);

ok($bdbh=$bdbw->create_read_dbh($bdb, { 'reverse'=>1 }));

my $value2;
$bdbh->db_get(4, $value2);
ok($value2==4);

my $bdb2='test2.bdb';
$write_hash_ref=$bdbw->create_write_hash_ref($bdb2);
$write_hash_ref->{'write'}=1;
undef $write_hash_ref;

my $hash_ref=$bdbw->create_read_hash_ref($bdb2);
ok($hash_ref->{'write'}==1);

my $new_bdbw=new BDB::Wrapper({'ram'=>1});
my $new_dbh;
ok($new_dbh=$new_bdbw->create_write_dbh('test3.bdb'));
ok($new_dbh->db_put('name', $value)==0);
$new_dbh->db_close();

