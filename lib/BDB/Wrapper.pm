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
our $VERSION = '0.11';

=head1 NAME

BDB::Wrapper Wrapper module for BerkeleyDB.pm

This will make it easy to use BerkeleyDB.pm.

You can protect bdb file from the concurrent access and you can use BerkeleyDB.pm with less difficulty.

=cut


=head2 new

Creates an object of BDB::Wrapper

=cut

sub new(){
  my $self={};
  my $class=shift;
  my $op_ref=shift;
  $self->{'lock_root'}='/tmp';
  if($op_ref->{'ram'}){
    $self->{'lock_root'}='/dev/shm';
  }
  $self->{'wait'}=11; #ŠJ‚­‚Ì‚ÉŽžŠÔ‚©‚©‚è‰ß‚¬
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
      -Home  => $home_dir
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

sub create_dbh(){
  my $self=shift;
  my $bdb=File::Spec->rel2abs(shift);
  my $op=shift;
  return $self->create_write_dbh($bdb,$op);
}

sub create_hash_ref(){
  my $self=shift;
  my $bdb=File::Spec->rel2abs(shift);
  my $op=shift;
  return $self->create_write_hash_ref($bdb, $op);
}

=head2

This will creates database handler for writing.

$self->create_write_dbh($bdb, {'hash'=>0 or 1, 'dont_try'=>0 or 1, 'sort_code_ref'=>$sort_code_reference, 'sort_num'=>0 or 1, 'reverse_cmp'=>0 or 1, 'reverse_num'=>0 or 1, 'reverse'=>0 or 1});

In the default mode, BDB file will be created as Btree;

If you set 'hash' 1, Hash BDB will be created.

If you set 'dont_try' 1, this module won\'t try to unlock BDB if it detects the situation in which deadlock may be occuring.

If you set sort_code_ref some code reference, you can set subroutine for sorting for Btree.

If you set sort or sort_num 1, you can use sub {$_[0] <=> $_[1]} for sort_code_ref.

If you set reverse 1, you can use sub {$_[1] <=> $_[0]} for sort_code_ref.

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


=head2

This will creates database handler for reading.

$self->create_read_dbh($bdb, {'hash'=>0 or 1, 'dont_try'=>0 or 1, 'sort_code_ref'=>$sort_code_reference, 'reverse_cmp'=>0 or 1, 'reverse'=>0 or 1});

In the default mode, BDB file will be created as Btree;

If you set 'hash' 1, Hash BDB will be created.

If you set 'dont_try' 1, this module won\'t try to unlock BDB if it detects the situation in which deadlock may be occuring.

If you set sort_code_ref some code reference, you can set subroutine for sorting for Btree.

If you set sort or sort_num 1, you can use sub {$_[0] <=> $_[1]} for sort_code_ref.

If you set reverse 1, you can use sub {$_[1] <=> $_[0]} for sort_code_ref.

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
  return $dbh;
}


=head2

This will creates hash for writing.

$self->create_write_hash_ref($bdb, {'hash'=>0 or 1, 'dont_try'=>0 or 1, 'sort_code_ref'=>$sort_code_reference, 'reverse_cmp'=>0 or 1, 'reverse'=>0 or 1});

In the default mode, BDB file will be created as Btree;

If you set 'hash' 1, Hash BDB will be created.

If you set 'dont_try' 1, this module won\'t try to unlock BDB if it detects the situation in which deadlock may be occuring.

If you set sort_code_ref some code reference, you can set subroutine for sorting for Btree.

If you set sort or sort_num 1, you can use sub {$_[0] <=> $_[1]} for sort_code_ref.

If you set reverse 1, you can use sub {$_[1] <=> $_[0]} for sort_code_ref.

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
  my %hash;
  
  local $SIG{ALRM} = sub { die "timeout"};
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

=head2

This will creates database handler for reading.

$self->create_read_hash_ref($bdb, 'hash'=>0 or 1, 'dont_try'=>0 or 1, 'sort_code_ref'=>$sort_code_reference, 'reverse_cmp'=>0 or 1, 'reverse'=>0 or 1});

In the default mode, BDB file will be created as Btree;

If you set 'hash' 1, Hash BDB will be created.

If you set 'dont_try' 1, this module won\'t try to unlock BDB if it detects the situation in which deadlock may be occuring.

If you set sort_code_ref some code reference, you can set subroutine for sorting for Btree.

If you set sort or sort_num 1, you can use sub {$_[0] <=> $_[1]} for sort_code_ref.

If you set reverse 1, you can use sub {$_[1] <=> $_[0]} for sort_code_ref.

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
  if($sort_code_ref && !$hash){
    tie %hash, $type,
    -Filename => $bdb,
    -Flags    => DB_RDONLY,
    -Compare => $sort_code_ref;
    return \%hash;
  }
  else{
    tie %hash, $type,
    -Filename => $bdb,
    -Flags    => DB_RDONLY;
    return \%hash;
  }
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
