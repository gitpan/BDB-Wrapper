package BDB::Wrapper;
use 5.006;
use strict;
use warnings;
use BerkeleyDB;
use Carp;
use File::Spec;
use FileHandle;
use Exporter;
use AutoLoader qw(AUTOLOAD);

our @ISA = qw(Exporter AutoLoader);
our %EXPORT_TAGS = ( 'all' => [ qw() ] );
our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );
our @EXPORT = qw();
our $VERSION = '0.33';

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

=cut

=pod

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

    if($dbh && $dbh->db_put('name', 'value')==0){

    }

    else{

      $dbh->db_close() if $dbh;

      die 'Failed to put to '.$self->{'bdb'};

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


If you set {'transaction'=>transaction_root_dir}, all dbh object will be created in transaction mode unless you don\'t specify transaction root dir in each method.

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
    elsif($key eq 'transaction'){
      $self->{'transaction'}=$value;
      if($self->{'transaction'} && $self->{'transaction'}!~ m!^/.!){
        croak("transaction parameter must be valid directory name.");
      }
      if($self->{'transaction'}){
        $self->{'lock_root'}=$self->{'transaction'};
      }
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

create_env({'bdb'=>$bdb, 'no_lock='>0(default) or 1, 'cache'=>undef(default) or integer, 'error_log_file'=>undef or $error_log_file, 'transaction'=> 0==undef or $transaction_root_dir});

no_lock and cache will overwrite the value specified in new but used only in this env

=cut

sub create_env(){
  my $self=shift;
  my $op=shift;
  my $bdb=File::Spec->rel2abs($op->{'bdb'}) || return;
  my $no_lock=$op->{'no_lock'} || $self->{'no_lock'} || 0;
  my $transaction=$undef;
  $self->{'error_log_file'}=$op->{'errore_log_file'};
  if(exists($op->{'transaction'})){
    $transaction=$op->{'transaction'};
  }
  else{
    $transaction=$self->{'transaction'};
  }
  if($transaction && $transaction!~ m!^/.!){
    croak("transaction parameter must be valid directory name.");
  }
  my $cache=$op->{'cache'} || $self->{'Cachesize'} || undef;
  my $env;
  my $Flags;
  if($transaction){
    if($transaction=~ m!^/.!){
      $Flags=DB_INIT_LOCK |DB_INIT_LOG | DB_INIT_TXN | DB_CREATE | DB_INIT_MPOOL;
    }
    else{
      croak("transaction parameter must be valid directory name.");
    }
  }
  elsif($no_lock){
    $Flags=DB_CREATE | DB_INIT_MPOOL;
  }
  else{
    $Flags=DB_INIT_CDB | DB_CREATE | DB_INIT_MPOOL;
  }
  my $lock_flag;
  my $home_dir=$self->get_bdb_home({'bdb'=>$bdb, 'transaction'=>$transaction});
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
      -LockDetect => $lock_flag,
      -Mode => 0666, 
      -ErrFile => $self->{'error_log_file'}
      };
  }
  else{
    $env = new BerkeleyDB::Env {
      -Flags => $Flags,
      -Home  => $home_dir,
      -LockDetect => $lock_flag,
      -Mode => 0666, 
      -ErrFile => $self->{'error_log_file'}
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

This returns database handler for writing or ($database_handler, $env) depeinding on the request.

$self->create_write_dbh({'bdb'=>$bdb, 'cache'=>undef(default) or integer, 'hash'=>0 or 1, 'dont_try'=>0 or 1,'no_lock'=>0(default) or 1, 'sort_code_ref'=>$sort_code_reference, 'sort' or 'sort_num'=>0 or 1, 'transaction'=> 0==undef or $transaction_root_dir, 'reverse_cmp'=>0 or 1, 'reverse' or 'reverse_num'=>0 or 1});

In the default mode, BDB file will be created as Btree;

If you set 'hash' 1, Hash BDB will be created.

If you set 'dont_try' 1, this module won\'t try to unlock BDB if it detects the situation in which deadlock may be occuring.

If you set sort_code_ref some code reference, you can set subroutine for sorting for Btree.

If you set sort or sort_num 1, you can use sub {$_[0] <=> $_[1]} for sort_code_ref.

If you set reverse or reverse_num 1, you can use sub {$_[1] <=> $_[0]} for sort_code_ref.

If you set reverse_cmp 1, you can use sub {$_[1] cmp $_[0]} for sort_code_ref.

If you set transaction for storing transaction log, transaction will be used and ($bdb_handler, $transaction_handler) will be returned.

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

  my $transaction = undef;
  my $hash=0;
  my $dont_try=0;
  my $sort_code_ref=undef;
  if(ref($op) eq 'HASH'){
    $hash=$op->{'hash'} || 0;
    $dont_try=$op->{'dont_try'} || 0;
    if(exists($op->{'transaction'})){
      $transaction = $op->{'transaction'};
    }
    else{
      $transaction = $self->{'transaction'};
    }
    if($transaction && $transaction!~ m!^/.!){
      croak("transaction parameter must be valid directory name.");
    }
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
    $env=$self->create_env({'bdb'=>$op->{'bdb'}, 'cache'=>$op->{'cache'}, 'no_lock'=>$op->{'no_lock'}, 'transaction'=>$transaction});
  }
  
  my $bdb_dir=$op->{'bdb'};
  $bdb_dir=~ s!/[^/]+$!!;
  
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
  
  if(!$dbh){
    {
      local $|=0;
      print "Content-type:text/html\n\n";
      print "Failed to create write dbh for ".$op->{'bdb'};
      exit;
    }
  }
  else{
    if(wantarray){
      return ($dbh, $env);
    }
    else{
      return $dbh;
    }
  }
}


=head2 create_read_dbh

This returns database handler for reading or ($database_handler, $env) depeinding on the request.

$self->create_read_dbh({'bdb'=>$bdb, 'hash'=>0 or 1, 'dont_try'=>0 or 1, 'sort_code_ref'=>$sort_code_reference, 'sort' or 'sort_num'=>0 or 1, 'reverse_cmp'=>0 or 1, 'reverse' or 'reverse_num'=>0 or 1, 'transaction'=> 0==undef or $transaction_root_dir});

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
  my $transaction=undef;
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
    if(exists($op->{'transaction'})){
      $transaction=$op->{'transaction'};
    }
    else{
      $transaction=$self->{'transaction'};
    }
    if($transaction && $transaction!~ m!^/.!){
      croak("transaction parameter must be valid directory name.");
    }
    
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
    elsif($op->{'sort_code_ref'}){
      $sort_code_ref=$op->{'sort_code_ref'};
    }
  }
  else{
    $hash=$op || 0;
    $dont_try=shift || 0;
    $sort_code_ref=shift;
  }
  
  my $env='';
  if($op->{'use_env'} || $transaction){
    $env=$self->create_env({'bdb'=>$op->{'bdb'}, 'cache'=>$op->{'cache'}, 'no_lock'=>$op->{'no_lock'}, 'transaction'=>$transaction});
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
        $self->clear_bdb_home({'bdb'=>$op->{'bdb'}, 'transaction'=>$transaction});
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
    
  if(!$dbh){
    return;
  }
  else{
    if(wantarray){
      return ($dbh, $env);
    }
    else{
      return $dbh;
    }
  }
}


=head2 create_write_hash_ref

Not recommended method. Please use create_write_dbh() instead of this method.

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
  
  my $bdb_dir=$bdb;
  $bdb_dir=~ s!/[^/]+$!!;
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
        my $home_dir=$self->get_bdb_home({'bdb'=>$bdb});
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
  my $op=shift;
  my $bdb='';
  my $transaction=undef;
  my $lock_root=$self->{'lock_root'};
  if($op && ref($op) eq 'HASH'){
    $bdb=$op->{'bdb'} || return;
    if(exists($op->{'transaction'})){
      $transaction=$op->{'transaction'};
    }
    else{
      $transaction=$self->{'transaction'};
    }
  }
  else{
    $bdb=File::Spec->rel2abs($op) || return;
    $transaction=$self->{'transaction'};
  }
  if($transaction && $transaction!~ m!^/.!){
    croak("transaction parameter must be valid directory name.");
  }
  if($transaction){
    $lock_root=$transaction;
  }
  if($bdb=~ s!\.bdb$!!i){
    return $lock_root.'/bdb_home'.$bdb;
  }
  else{
    croak("BDB file's name must be ended with .bdb for verification.");
  }
}


=head2 clear_bdb_home

This will clear bdb_home.

get_bdb_home({'bdb'=>$bdb, 'transaction' => 0==undef or $transaction_root_dir});

=cut

sub clear_bdb_home(){
  my $self=shift;
  my $op=shift;
  my $bdb='';
  my $transaction=undef;
  my $lock_root=$self->{'lock_root'};
  if($op && ref($op) eq 'HASH'){
    $bdb=$op->{'bdb'} || return;
    if(exists($op->{'transaction'})){
      $transaction=$op->{'transaction'};
    }
    else{
      $transaction=$self->{'transaction'};
    }
    if($transaction && $transaction!~ m!^/.!){
      croak("transaction parameter must be valid directory name.");
    }
    if($transaction){
      $lock_root=$transaction;
    }
  }
  else{
    $bdb=File::Spec->rel2abs($op) || return;
  }
  if($bdb=~ s!\.bdb$!!i){
    my $dir=$lock_root.'/bdb_home'.$bdb;
    my $dh;
    opendir($dh, $dir);
    if($dh){
      while (my $file = readdir $dh){
        if(-f $dir.'/'.$file){
          unlink $dir.'/'.$file;
        }
      }
      closedir $dh;
      rmdir $dir;
    }
  }
  else{
    croak("BDB file's name must be ended with .bdbfor verification.");
  }
}

=head2 record_error

This will record error message to /tmp/bdb_error.log if you don\'t specify error_log_file

record_error({'msg'=>$error_message, 'error_log_file'=>$error_log_file);

OR

record_error($error_msg)

=cut

sub record_error(){
  my $self=shift;
  my $op=shift || return;
  my $msg='';
  my $error_log_file='';
  
  if($op && ref($op) eq 'HASH'){
    $msg=$op->{'msg'};
    $error_log_file=$op->{'error_log_file'};
  }
  else{
    $msg=$op;
  }
  if(!$error_log_file){
    if($self->{'error_log_file'}){
      $error_log_file=$self->{'error_log_file'};
    }
    else{
      $error_log_file='/tmp/bdb_error.log';
    }
  }
  if(my $fh=new FileHandle('>> '.$error_log_file)){
    my ($in_sec,$in_min,$in_hour,$in_mday,$in_mon,$in_year,$in_wday)=localtime(CORE::time());
    $in_mon++;
    $in_year+=1900;
    $in_mon='0'.$in_mon if($in_mon<10);
    $in_mday='0'.$in_mday if($in_mday<10);
    $in_hour='0'.$in_hour if($in_hour<10);
    $in_min='0'.$in_min if($in_min<10);
    $in_sec='0'.$in_sec if($in_sec<10);
    print $fh $in_year.'/'.$in_mon.'/'.$in_mday.' '.$in_hour.':'.$in_min.':'.$in_sec."\t".$msg."\n";
    $fh->close();
  }
}
