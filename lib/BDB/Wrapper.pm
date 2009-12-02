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
our $VERSION = '0.26';

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
  if(my $dbh=$self->{'bdbw'}->create_write_dbh($self->{'bdb'})){
    local $SIG{'INT'};
    local $SIG{'TERM'};
    local $SIG{'QUIT'};
    $SIG{'INT'}=$SIG{'TERM'}=$SIG{'QUIT'}=sub {$dbh->db_close();};
    if($dbh){
      if($dbh->db_put('name', 'value')==0){
      }
      else{
        $dbh->db_close() if $dbh;
        die 'Failed to put to '.$self->{'bdb'};
      }
    }
    $dbh->db_close() if $dbh;
  }

  if(my $dbh=$self->{'bdbw'}->create_read_dbh($self->{'bdb'})){
    my $value;
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

If you set {'ram'=>1}, you can use /dev/shm/bdb_home for storing locking file for BDB instead of /tmp/bdb_home/.

1 is default value.

If you set {'no_lock'=>1}, the control of concurrent access will not be used. So the lock files are also not created.

0 is default value.

If you set {'cache'=>$CACHE_SIZE}, you can allocate cache memory of the specified bytes for using bdb files.

The value can be overwritten by the cache value of create_write_dbh

undef is default value.

If you set {'wait'=>wait_seconds}, you can specify the seconds in which dead lock will be removed.

11 is default value.

=cut

sub new(){
  my $self={};
  my $class=shift;
  my $op_ref=shift;
  $self->{'lock_root'}='/tmp';
  $self->{'no_lock'}=0;
  $self->{'Flags'}='';
  $self->{'wait'}= 11;
  while(my ($key, $value)=each %{$op_ref}){
    if($key eq 'ram'){
      if($value){
        $self->{'lock_root'}='/dev/shm';
      }
    }
    elsif($key eq 'cache'){
      $self->{'Cachesize'}=$value if(defined($value));
    }
    elsif($key eq 'Cachesize'){
      # Cachesize eq undef ‚Í“®ì‚ÌŽd•û‚ª•s–¾‚È‚Ì‚Å
      $self->{'Cachesize'}=$value if(defined($value));
    }
    elsif($key eq 'no_lock'){
      if($value){
        $self->{'no_lock'}++;
      }
    }
    elsif($key eq 'wait'){
      $self->{'wait'}=$value;
    }
    else{
      my $error='Invalid option: key='.$key;
      if($value){
        $error.=', value='.$value;
      }
      Carp::croak($error);
    }
  }
  return bless $self;
}

1;
__END__

=head2 create_env

Creates Environment for BerkeleyDB

create_env({'bdb'=>$bdb, 'no_lock='>0(default) or 1, 'cache'=>undef(default) or integer});

no_lock and cache will overwrite the value specified in new but used only in this env

=cut

sub create_env(){
  my $self=shift;
  my $op=shift;
  my $bdb=File::Spec->rel2abs($op->{'bdb'}) || return;
  my $no_lock=$op->{'no_lock'} || $self->{'no_lock'} || 0;
  my $cache=$op->{'cache'} || $self->{'cache'} || undef;
  my $env;
  my $Flags;
  if($no_lock){
    $Flags=DB_CREATE | DB_INIT_MPOOL;
  }
  else{
    $Flags=DB_INIT_CDB | DB_CREATE | DB_INIT_MPOOL;
  }
  my $bdb_dir=$bdb;
  $bdb_dir=~ s!/[^/]+$!!;
  my $lock_flag;
  my $home_dir=$self->get_bdb_home($bdb);
  $home_dir=~ s!\.[^/\.\s]+$!!;
  unless(-d $home_dir){
    $self->rmkdir($home_dir);
  }
  
  $lock_flag=DB_LOCK_OLDEST unless($no_lock);
  if($cache){
    $env = new BerkeleyDB::Env {
      -Cachesize => $cache,
      -Flags => $Flags,
      -Home  => $home_dir,
      -LockDetect => $lock_flag
      };
  }
  else{
    $env = new BerkeleyDB::Env {
      -Flags => $Flags,
      -Home  => $home_dir,
      -LockDetect => $lock_flag
      };
  }
  # DB_CREATE is necessary for ccdb
  # Home is necessary for locking
  return $env;
}
# { DB_DATA_DIR => "/home/databases",                          DB_LOG_DIR  => "/home/logs",                          DB_TMP_DIR  => "/home/tmp"


=head2 create_dbh

Not recommened method. Please use create_read_dbh() or create_write_dbh().

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

Not recommended method. Please use create_write_dbh().

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

Not recommended:

$self->create_write_dbh($bdb, {'hash'=>0 or 1, 'dont_try'=>0 or 1, 'sort_code_ref'=>$sort_code_reference, 'sort' or 'sort_num'=>0 or 1, 'reverse_cmp'=>0 or 1, 'reverse' or 'reverse_num'=>0 or 1});

OR

Recommended:

$self->create_write_dbh({'bdb'=>$bdb, 'cache'=>undef(default) or integer, 'hash'=>0 or 1, 'dont_try'=>0 or 1,'no_lock'=>0(default) or 1, 'sort_code_ref'=>$sort_code_reference, 'sort' or 'sort_num'=>0 or 1, 'reverse_cmp'=>0 or 1, 'reverse' or 'reverse_num'=>0 or 1});

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
  my $bdb=shift;
  my $op='';
  if($bdb && ref($bdb) eq 'HASH'){
    $op=$bdb;
    $bdb=$op->{'bdb'};
  }
  else{
    $op=shift;
    $op->{'bdb'}=$bdb;
  }
  
  $op->{'bdb'}=File::Spec->rel2abs($op->{'bdb'});
  
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
      $sort_code_ref=sub {$_[0] <=> $_[1]};
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
  my $env;

  if($op->{'no_env'}){
    $env=undef;
  }
  else{
    $env=$self->create_env({'bdb'=>$op->{'bdb'}, 'cache'=>$op->{'cache'}, 'no_lock'=>$op->{'no_lock'}});
  }
  
  my $dbh;
  $SIG{ALRM} = sub { die "timeout"};
  eval{
    alarm($self->{'wait'});
    $self->rmkdir($bdb_dir);
    if($hash){
      $dbh =new BerkeleyDB::Hash {
        -Filename => $op->{'bdb'},
        -Flags => DB_CREATE,
        -Mode => 0666,
        -Env => $env
        };
    }
    else{
      $dbh =new BerkeleyDB::Btree {
        -Filename => $op->{'bdb'},
        -Flags => DB_CREATE,
        -Mode => 0666,
        -Env => $env,
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
        my $home_dir=$self->get_bdb_home($op->{'bdb'});
        system('rm -rf '.$home_dir) if ($home_dir=~ m!^(?:/tmp|/dev/shm)!);
        if(ref($op) eq 'HASH'){
          return $self->create_write_dbh($op->{'bdb'}, $op);
        }
        else{
          return $self->create_write_dbh($op->{'bdb'}, $hash, $dont_try, $sort_code_ref);
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

OR

$self->create_read_dbh({'bdb'=>$bdb, 'hash'=>0 or 1, 'dont_try'=>0 or 1, 'sort_code_ref'=>$sort_code_reference, 'sort' or 'sort_num'=>0 or 1, 'reverse_cmp'=>0 or 1, 'reverse' or 'reverse_num'=>0 or 1});

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
  my $bdb=shift;
  my $op='';
  if($bdb && ref($bdb) eq 'HASH'){
    $op=$bdb;
    $bdb=$op->{'bdb'};
  }
  else{
    $op=shift;
    $op->{'bdb'}=$bdb;
  }
  $op->{'bdb'}=File::Spec->rel2abs($op->{'bdb'});
  
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
      $sort_code_ref=sub {$_[0] <=> $_[1]};
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

  my $env='';
  if($op->{'use_env'}){
    $env=$self->create_env({'bdb'=>$op->{'bdb'}});
  }
  else{
    $env=undef;
  }
  
  my $dbh;
  $SIG{ALRM} = sub { die "timeout"};
  eval{
    alarm($self->{'wait'});
    if($hash){
      $dbh =new BerkeleyDB::Hash {
        -Env=>$env,
        -Filename => $op->{'bdb'},
        -Flags    => DB_RDONLY
        };
    }
    else{
      $dbh =new BerkeleyDB::Btree {
        -Env=>$env,
        -Filename => $op->{'bdb'},
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
        my $home_dir=$self->get_bdb_home($op->{'bdb'});
        system('rm -rf '.$home_dir) if ($home_dir=~ m!^(?:/tmp|/dev/shm)!);
        if(ref($op) eq 'HASH'){
          return $self->create_read_dbh($op->{'bdb'}, $op);
        }
        else{
          return $self->create_read_dbh($op->{'bdb'}, $hash, $dont_try, $sort_code_ref);
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

Not recommended method. Please use create_write_dbh().

This will creates hash for writing.

$self->create_write_hash_ref($bdb, {'hash'=>0 or 1, 'dont_try'=>0 or 1, 'sort_code_ref'=>$sort_code_reference,  'sort' or 'sort_num'=>0 or 1, 'reverse_cmp'=>0 or 1, 'reverse' or 'reverse_num'=>0 or 1});

OR

$self->create_write_hash_ref({'bdb'=>$bdb, 'hash'=>0 or 1, 'dont_try'=>0 or 1, 'sort_code_ref'=>$sort_code_reference,  'sort' or 'sort_num'=>0 or 1, 'reverse_cmp'=>0 or 1, 'reverse' or 'reverse_num'=>0 or 1});


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
  my $bdb=shift;
  my $op='';
  if($bdb && ref($bdb) eq 'HASH'){
    $op=$bdb;
    $bdb=$op->{'bdb'};
  }
  else{
    $op=shift;
  }
  $bdb=File::Spec->rel2abs($bdb);
  
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
      $sort_code_ref=sub {$_[0] <=> $_[1]};
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
  my $env;
  if($self->{'op'}->{'no_env'}){
    $env=undef;
  }
  else{
    $env=$self->create_env({'bdb'=>$bdb});
  }
  
  local $SIG{ALRM} = sub { die "timeout"};
  my %hash;
  eval{
    alarm($self->{'wait'});
    $self->rmkdir($bdb_dir);
    if($sort_code_ref && !$hash){
      tie %hash, $type,
      -Env=>$env,
      -Filename => $bdb,
      -Mode => 0666,
      -Flags    => DB_CREATE,
      -Compare => $sort_code_ref;
    }
    else{
      tie %hash, $type,
      -Env=>$env,
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
        my $home_dir=$self->get_bdb_home($bdb);
        system('rm -rf '.$home_dir) if ($home_dir=~ m!^(?:/tmp|/dev/shm)!);
        if(ref($op) eq 'HASH'){
          return $self->create_write_hash_ref($bdb, $op);
        }
        else{
          return $self->create_write_hash_ref($bdb, $hash, $dont_try, $sort_code_ref);
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

Not recommended method. Please use create_read_dbh and cursor().

This will creates database handler for reading.

$self->create_read_hash_ref($bdb, {'hash'=>0 or 1, 'dont_try'=>0 or 1, 'sort_code_ref'=>$sort_code_reference, 'sort' or 'sort_num'=>0 or 1, 'reverse_cmp'=>0 or 1, 'reverse' or 'reverse_num'=>0 or 1});

OR

$self->create_read_hash_ref({'bdb'=>$bdb, 'hash'=>0 or 1, 'dont_try'=>0 or 1, 'sort_code_ref'=>$sort_code_reference, 'sort' or 'sort_num'=>0 or 1, 'reverse_cmp'=>0 or 1, 'reverse' or 'reverse_num'=>0 or 1});

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
  my $bdb=shift;
  my $op='';
  if($bdb && ref($bdb) eq 'HASH'){
    $op=$bdb;
    $bdb=$op->{'bdb'};
  }
  else{
    $op=shift;
  }
  $bdb=File::Spec->rel2abs($bdb);
  
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
      $sort_code_ref=sub {$_[0] <=> $_[1]};
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
  
  my $env='';
  if($self->{'op'}->{'no_env'}){
    $env=undef;
  }
  else{
    $env=$self->create_env({'bdb'=>$bdb});
  }
  
  my %hash;
  local $SIG{ALRM} = sub { die "timeout"};
  eval{
    alarm($self->{'wait'});
    if($sort_code_ref && !$hash){
      tie %hash, $type,
      -Env=>$env,
      -Filename => $bdb,
      -Flags    => DB_RDONLY,
      -Compare => $sort_code_ref;
    }
    else{
      tie %hash, $type,
      -Env=>$env,
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
        my $home_dir=$self->get_bdb_home($bdb);
        system('rm -rf '.$home_dir) if($home_dir=~ m!^(?:/tmp|/dev/shm)!);
        if(ref($op) eq 'HASH'){
          return $self->create_read_hash_ref($bdb, $op);
        }
        else{
          return $self->create_read_hash_ref($bdb, $hash, $dont_try, $sort_code_ref);
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


=head2 get_bdb_home

This will return bdb_home.

You may need the information for recovery and so on.

get_bdb_home($BDB);

=cut

sub get_bdb_home(){
  my $self=shift;
  my $bdb=File::Spec->rel2abs(shift) || return;;
  $bdb=~ s!\.bdb$!!i;
  return $self->{'lock_root'}.'/bdb_home'.$bdb;
}
