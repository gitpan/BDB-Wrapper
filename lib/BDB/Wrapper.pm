package BDB::Wrapper;
use 5.006;
use strict;
use warnings;
use BerkeleyDB;
use Carp;
use File::Spec;
use Exporter;
use AutoLoader qw(AUTOLOAD);

our @ISA = qw(Exporter AutoLoader);
our %EXPORT_TAGS = ( 'all' => [ qw() ] );
our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );
our @EXPORT = qw();
our $VERSION = '0.16';

=head1 NAME

BDB::Wrapper Wrapper module for BerkeleyDB.pm

This will make it easy to use BerkeleyDB.pm.

You can protect bdb file from the concurrent access and you can use BerkeleyDB.pm with less difficulty.

This module is used on http://www.accessup.org/ and is developed based on the requirement.


Attention: If you use this module for the specified Berkeley DB file,

please use this module for all access to the bdb.

By it, you can control lock to the bdb file.

Lock files are created under /tmp/bdb_home.

If you set ram 1 in new option, lock files are created under /dev/shm/bdb_home.

=cut

=head1 Example

  # Code Example
package test_bdb;
use BDB::Wrapper;

my $pro=new test_bdb;
$pro->run();

sub new(){
  my $self={};
  return bless $self;
}

sub run(){
  my $self=shift;
  $self->init_vars();
  $self->demo();
}

sub init_vars(){
  my $self=shift;
  $self->{'bdb'}='/tmp/test.bdb';
  $self->{'bdbw'}=new BDB::Wrapper;
}

sub demo(){
  my $self=shift;
  # Open db handler for writing
  if(my $dbh=$self->{'bdbw'}->create_write_dbh($self->{'bdb'})){
    # Ignore Ctr+C while putting to avoid to destruct bdb.
    local $SIG{'INT'}='IGNORE';
    local $SIG{'TERM'}='IGNORE';
    local $SIG{'QUIT'}='IGNORE';
    # put
    if($dbh->db_put('name', 'value')==0){
    }
    else{
      die 'Failed to put to '.$self->{'bdb'};
    }
    $dbh->db_close();
  }

  # Open db handler for reading
  if(my $dbh=$self->{'bdbw'}->create_read_dbh($self->{'bdb'})){
    my $value;
    # read
    if($dbh->db_get('name', $value)==0){
      print 'Name='.$name.' value='.$value."\n";
    }
    $dbh->db_close();
  }
}

=cut

=head1 methods

=head2 new

Creates an object of BDB::Wrapper

If you set {'ram'=>1}, you can use /dev/shm for storing locking file for BDB.

If you set {'wait'=>wait_seconds}, you can specify the seconds in which dead lock will be removed.

=cut

sub new(){
  my $self={};
  my $class=shift;
  my $op_ref=shift;
  $self->{'lock_root'}='/tmp';
  if($op_ref->{'ram'}){
    $self->{'lock_root'}='/dev/shm';
  }
  $self->{'wait'}=$op_ref->{'wait'} || 11; #ŠJ‚­‚Ì‚ÉŽžŠÔ‚©‚©‚è‰ß‚¬
  return bless $self;
}

1;
__END__

=head2 create_env

Creates Environment for BerkeleyDB

=cut

sub create_env(){
  my $self=shift;
  my $bdb=File::Spec->rel2abs(shift) || return;
  my $env;
  $bdb=~ s!\.bdb$!!i;
  my $home_dir='';
  
  if($bdb=~ m!^/!){
    $home_dir=$self->{'lock_root'}.'/bdb_home'.$bdb; # ˆê‰ñŒÀ‚è‚ÌBDB‚É‚Í–³‘Ê
    unless(-d $home_dir){
      $self->rmkdir($home_dir);
    }
    $env = new BerkeleyDB::Env {
      -Flags => DB_INIT_CDB | DB_CREATE | DB_INIT_MPOOL,
      -Home => $home_dir
      };
  }
  else{
    # rel2abs‚µ‚Ä‚¢‚é‚©‚ç‚»‚ñ‚È‚±‚Æ‚È‚¢”¤‚¾‚¯‚Ç
    $env = new BerkeleyDB::Env {
      -Flags => DB_INIT_CDB | DB_CREATE | DB_INIT_MPOOL
      };
  }
  # DB_CREATE is necessary for ccdb
  # Home is necessary for locking
  return $env;
}
# { DB_DATA_DIR => "/home/databases",                          DB_LOG_DIR  => "/home/logs",                          DB_TMP_DIR  => "/home/tmp"


=head2 create_dbh

Creates database handler for BerkeleyDB

This will be obsolete due to too much simplicity, so please don\'t use.

=cut

sub create_dbh(){
  my $self=shift;
  my $bdb=File::Spec->rel2abs(shift);
  my $op=shift;
  return $self->create_write_dbh($bdb,$op);
}

=head2 create_hash_ref

Creates database handler for BerkeleyDB

This will be obsolete due to too much simplicity, so please don\'t use.

=cut

sub create_hash_ref(){
  my $self=shift;
  my $bdb=File::Spec->rel2abs(shift);
  my $op=shift;
  return $self->create_write_hash_ref($bdb, $op);
}

=head2 create_write_dbh

This will creates database handler for writing.

$self->create_write_dbh($bdb, {'hash'=>0 or 1, 'dont_try'=>0 or 1, 'sort_code_ref'=>$sort_code_reference, 'sort' or 'sort_num'=>0 or 1, 'reverse_cmp'=>0 or 1, 'reverse' or 'reverse_num'=>0 or 1});

In the default mode, BDB file will be created as Btree;

If you set 'hash' 1, Hash BDB will be created.

If you set 'dont_try' 1, this module won\'t try to unlock BDB if it detects the situation in which deadlock may be occuring.

If you set sort_code_ref some code reference, you can set subroutine for sorting for Btree.

If you set sort or sort_num 1, you can use sub {$_[0] <=> $_[1]} for sort_code_ref.

If you set reverse or reverse_num 1, you can use sub {$_[1] <=> $_[0]} for sort_code_ref.

If you set reverse_cmp 1, you can use sub {$_[1] cmp $_[0]} for sort_code_ref.

=cut

sub create_write_dbh(){
  my $self=shift;
  my $bdb=File::Spec->rel2abs(shift);
  my $op=shift;
  
  my $hash=0;
  my $dont_try=0;
  my $sort_code_ref=undef;
  if(ref($op) eq 'HASH'){
    $hash=$op->{'hash'} || 0;
    $dont_try=$op->{'dont_try'} || 0;
    if($op->{'reverse'} || $op->{'reverse_num'}){
      $sort_code_ref=sub {$_[1] <=> $_[0]};
    }
    elsif($op->{'reverse_cmp'}){
      $sort_code_ref=sub {$_[1] cmp $_[0]};
    }
    elsif($op->{'sort'} || $op->{'sort_num'}){
      $sort_code_ref=sub {$_[0] cmp $_[1]};
    }
    else{
      $sort_code_ref=$op->{'sort_code_ref'};
    }
  }
  else{
    $hash=$op || 0;
    $dont_try=shift || 0;
    $sort_code_ref=shift;
  }
  my $dbh;
  $SIG{ALRM} = sub { die "timeout"};
  eval{
    alarm($self->{'wait'});
    if($hash){
      $dbh =new BerkeleyDB::Hash {
        -Filename => $bdb,
        -Flags    => DB_CREATE,
        -Mode => 0666,
        -Env => $self->create_env($bdb)};
    }
    else{
      $dbh =new BerkeleyDB::Btree {
        -Filename => $bdb,
        -Flags    => DB_CREATE,
        -Mode => 0666,
        -Env => $self->create_env($bdb),
        -Compare => $sort_code_ref
        };
    }
    alarm(0);
  };
  
  unless($dont_try){
    if($@){
      if($@ =~ /timeout/){
        $op->{'dont_try'}=1;
        $dont_try=1;
        my $home_dir=$bdb;
        if($home_dir=~ s![^/]+$!!){
          my $i=1;
          my $lock=$home_dir.'__db.00'.$i;
          while(-f $lock){
            unlink $lock;
            $i++;
            $lock=$home_dir.'__db.00'.$i;
          }
          if(ref($op) eq 'HASH'){
            return $self->create_write_dbh($bdb, $op);
          }
          else{
            return $self->create_write_dbh($bdb, $hash, $dont_try, $sort_code_ref);
          }
        }
      }
      else{
        alarm(0);
      }
    }
  }
  return $dbh;
}


=head2 create_read_dbh

This will creates database handler for reading.

$self->create_read_dbh($bdb, {'hash'=>0 or 1, 'dont_try'=>0 or 1, 'sort_code_ref'=>$sort_code_reference, 'sort' or 'sort_num'=>0 or 1, 'reverse_cmp'=>0 or 1, 'reverse' or 'reverse_num'=>0 or 1});

In the default mode, BDB file will be created as Btree;

If you set 'hash' 1, Hash BDB will be created.

If you set 'dont_try' 1, this module won\'t try to unlock BDB if it detects the situation in which deadlock may be occuring.

If you set sort_code_ref some code reference, you can set subroutine for sorting for Btree.

If you set sort or sort_num 1, you can use sub {$_[0] <=> $_[1]} for sort_code_ref.

If you set reverse or reverse_num 1, you can use sub {$_[1] <=> $_[0]} for sort_code_ref.

If you set reverse_cmp 1, you can use sub {$_[1] cmp $_[0]} for sort_code_ref.

=cut

sub create_read_dbh(){
  my $self=shift;
  my $bdb=File::Spec->rel2abs(shift);
  my $op=shift;
  my $hash=0;
  my $dont_try=0;
  my $sort_code_ref=undef;
  if(ref($op) eq 'HASH'){
    $hash=$op->{'hash'} || 0;
    $dont_try=$op->{'dont_try'} || 0;
    if($op->{'reverse'} || $op->{'reverse_num'}){
      $sort_code_ref=sub {$_[1] <=> $_[0]};
    }
    elsif($op->{'reverse_cmp'}){
      $sort_code_ref=sub {$_[1] cmp $_[0]};
    }
    elsif($op->{'sort'} || $op->{'sort_num'}){
      $sort_code_ref=sub {$_[0] cmp $_[1]};
    }
    else{
      $sort_code_ref=$op->{'sort_code_ref'};
    }
  }
  else{
    $hash=$op || 0;
    $dont_try=shift || 0;
    $sort_code_ref=shift;
  }
  
  my $dbh;
  $SIG{ALRM} = sub { die "timeout"};
  eval{
    alarm($self->{'wait'});
    if($hash){
      $dbh =new BerkeleyDB::Hash {
        -Filename => $bdb,
        -Flags    => DB_RDONLY
        };
    }
    else{
      $dbh =new BerkeleyDB::Btree {
        -Filename => $bdb,
        -Flags    => DB_RDONLY,
        -Compare => $sort_code_ref
        };
    }
    alarm(0);
  };

  unless($dont_try){
    if($@){
      if($@ =~ /timeout/){
        $op->{'dont_try'}=1;
        $dont_try=1;
        my $home_dir=$bdb;
        if($home_dir=~ s![^/]+$!!){
          my $i=1;
          my $lock=$home_dir.'__db.00'.$i;
          while(-f $lock){
            unlink $lock;
            $i++;
            $lock=$home_dir.'__db.00'.$i;
          }
          if(ref($op) eq 'HASH'){
            return $self->create_read_dbh($bdb, $op);
          }
          else{
            return $self->create_read_dbh($bdb, $hash, $dont_try, $sort_code_ref);
          }
        }
      }
      else{
        alarm(0);
      }
    }
  }
  return $dbh;
}


=head2 create_write_hash_ref

This will creates hash for writing.

$self->create_write_hash_ref($bdb, {'hash'=>0 or 1, 'dont_try'=>0 or 1, 'sort_code_ref'=>$sort_code_reference,  'sort' or 'sort_num'=>0 or 1, 'reverse_cmp'=>0 or 1, 'reverse' or 'reverse_num'=>0 or 1});

In the default mode, BDB file will be created as Btree;

If you set 'hash' 1, Hash BDB will be created.

If you set 'dont_try' 1, this module won\'t try to unlock BDB if it detects the situation in which deadlock may be occuring.

If you set sort_code_ref some code reference, you can set subroutine for sorting for Btree.

If you set sort or sort_num 1, you can use sub {$_[0] <=> $_[1]} for sort_code_ref.

If you set reverse or reverse_num 1, you can use sub {$_[1] <=> $_[0]} for sort_code_ref.

If you set reverse_cmp 1, you can use sub {$_[1] cmp $_[0]} for sort_code_ref.

=cut

sub create_write_hash_ref(){
  my $self=shift;
  my $bdb=File::Spec->rel2abs(shift);
  my $op=shift;
  my $hash=0;
  my $dont_try=0;
  my $sort_code_ref=undef;
  if(ref($op) eq 'HASH'){
    $hash=$op->{'hash'} || 0;
    $dont_try=$op->{'dont_try'} || 0;
    if($op->{'reverse'} || $op->{'reverse_num'}){
      $sort_code_ref=sub {$_[1] <=> $_[0]};
    }
    elsif($op->{'reverse_cmp'}){
      $sort_code_ref=sub {$_[1] cmp $_[0]};
    }
    elsif($op->{'sort'} || $op->{'sort_num'}){
      $sort_code_ref=sub {$_[0] cmp $_[1]};
    }
    else{
      $sort_code_ref=$op->{'sort_code_ref'};
    }
  }
  else{
    $hash=$op || 0;
    $dont_try=shift || 0;
    $sort_code_ref=shift;
  }
  my $type='BerkeleyDB::Btree';
  if($hash){
    $type='BerkeleyDB::Hash';
  }
  local $SIG{ALRM} = sub { die "timeout"};
  my %hash;
  eval{
    alarm($self->{'wait'});
    if($sort_code_ref && !$hash){
      tie %hash, $type,
      -Env=>$self->create_env($bdb),
      -Filename => $bdb,
      -Mode => 0666,
      -Flags    => DB_CREATE,
      -Compare => $sort_code_ref;
    }
    else{
      tie %hash, $type,
      -Env=>$self->create_env($bdb),
      -Filename => $bdb,
      -Mode => 0666,
      -Flags    => DB_CREATE;
    }
    alarm(0);
  };
  
  unless($dont_try){
    if($@){
      if($@ =~ /timeout/){
        $op->{'dont_try'}=1;
        $dont_try=1;
        my $home_dir=$bdb;
        if($home_dir=~ s![^/]+$!!){
          my $i=1;
          my $lock=$home_dir.'__db.00'.$i;
          while(-f $lock){
              unlink $lock;
            $i++;
            $lock=$home_dir.'__db.00'.$i;
          }
          if(ref($op) eq 'HASH'){
            return $self->create_write_hash_ref($bdb, $op);
          }
          else{
            return $self->create_write_hash_ref($bdb, $hash, $dont_try, $sort_code_ref);
          }
        }
      }
      else{
        alarm(0);
      }
    }
  }
  return \%hash;
}

=head2 create_read_hash_ref

This will creates database handler for reading.

$self->create_read_hash_ref($bdb, 'hash'=>0 or 1, 'dont_try'=>0 or 1, 'sort_code_ref'=>$sort_code_reference, 'sort' or 'sort_num'=>0 or 1, 'reverse_cmp'=>0 or 1, 'reverse' or 'reverse_num'=>0 or 1});

In the default mode, BDB file will be created as Btree;

If you set 'hash' 1, Hash BDB will be created.

If you set 'dont_try' 1, this module won\'t try to unlock BDB if it detects the situation in which deadlock may be occuring.

If you set sort_code_ref some code reference, you can set subroutine for sorting for Btree.

If you set sort or sort_num 1, you can use sub {$_[0] <=> $_[1]} for sort_code_ref.

If you set reverse or reverse_num 1, you can use sub {$_[1] <=> $_[0]} for sort_code_ref.

If you set reverse_cmp 1, you can use sub {$_[1] cmp $_[0]} for sort_code_ref.

=cut

sub create_read_hash_ref(){
  my $self=shift;
  my $bdb=File::Spec->rel2abs(shift);
  my $op=shift;
  my $hash=0;
  my $dont_try=0;
  my $sort_code_ref=undef;
  if(ref($op) eq 'HASH'){
    $hash=$op->{'hash'} || 0;
    $dont_try=$op->{'dont_try'} || 0;
    if($op->{'reverse'} || $op->{'reverse_num'}){
      $sort_code_ref=sub {$_[1] <=> $_[0]};
    }
    elsif($op->{'reverse_cmp'}){
      $sort_code_ref=sub {$_[1] cmp $_[0]};
    }
    elsif($op->{'sort'} || $op->{'sort_num'}){
      $sort_code_ref=sub {$_[0] cmp $_[1]};
    }
    else{
      $sort_code_ref=$op->{'sort_code_ref'};
    }
  }
  else{
    # Obsolete
    $hash=$op || 0;
    $dont_try=shift || 0;
    $sort_code_ref=shift;
  }
  my $type='BerkeleyDB::Btree';
  if($hash){
    $type='BerkeleyDB::Hash';
  }

  my %hash;
  local $SIG{ALRM} = sub { die "timeout"};
  eval{
    alarm($self->{'wait'});
    if($sort_code_ref && !$hash){
      tie %hash, $type,
      -Filename => $bdb,
      -Flags    => DB_RDONLY,
      -Compare => $sort_code_ref;
    }
    else{
      tie %hash, $type,
      -Filename => $bdb,
      -Flags    => DB_RDONLY;
    }
    alarm(0);
  };
  
  unless($dont_try){
    if($@){
      if($@ =~ /timeout/){
        $op->{'dont_try'}=1;
        $dont_try=1;
        my $home_dir=$bdb;
        if($home_dir=~ s![^/]+$!!){
          my $i=1;
          my $lock=$home_dir.'__db.00'.$i;
          while(-f $lock){
            unlink $lock;
            $i++;
            $lock=$home_dir.'__db.00'.$i;
          }
          if(ref($op) eq 'HASH'){
            return $self->create_read_hash_ref($bdb, $op);
          }
          else{
            return $self->create_read_hash_ref($bdb, $hash, $dont_try, $sort_code_ref);
          }
        }
      }
      else{
        alarm(0);
      }
    }
  }
  return \%hash;
}

# Code from CGI::Accessup;
sub rmkdir(){
  my $self=shift;
  my $path=shift;
  my $force=shift;
  if($path){
    $path=~ s!^\s+|\s+$!!gs;
    if($path=~ m![^/\.]!){
      my $target='';
      if($path=~ s!^([\./]+)!!){
        $target=$1;
      }
      while($path=~ s!^([^/]+)/?!!){
        $target.=$1;
        if($force && -f $target){
          unlink $target;
        }
        unless(-d $target){
          mkdir($target,0777) || Carp::carp("Failed to create ".$target);
          # for avoiding umask to mkdir
          chmod 0777, $target || Carp::carp("Failed to chmod ".$target);;
        }
        $target.='/';
      }
      return 1;
    }
  }
  return 0;
}
