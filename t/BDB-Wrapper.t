# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl BDB-Wrapper.t'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use Test;
BEGIN { plan tests => 8 };
use BDB::Wrapper;
ok(1); # If we made it this far, we're ok.

#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.
my $bdbw;
ok($bdbw=new BDB::Wrapper);

my $bdb='test.bdb';
unlink $bdb if -f $bdb;
my $bdbh;
ok($bdbh=$bdbw->create_write_dbh($bdb));

ok($bdbh->db_put(1, 2)==0);

ok($bdbh->db_close()==0);

ok($bdbh=$bdbw->create_read_dbh($bdb));

my $value;
ok($bdbh->db_get(1, $value)==0 && $value==2);

my $bdb2='test2.bdb';
$write_hash_ref=$bdbw->create_write_hash_ref($bdb2);
$write_hash_ref->{'write'}=1;
undef $write_hash_ref;

my $hash_ref=$bdbw->create_read_hash_ref($bdb2);
ok($hash_ref->{'write'}==1);

