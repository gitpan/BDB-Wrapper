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
our $VERSION = '0.08';

=head1 NAME

BDB::Wrapper Wrapper module for BerkeleyDB.pm

This will make it easy to use BerkeleyDB.pm.

You can protect bdb file from the concurrent access and you can use BerkeleyDB.pm with less difficulty.

=cut


sub new(){
  my $self={};
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
    $home_dir='/tmp/bdb_home'.$bdb;
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

$self->create_write_dbh($bdb, ['hash'], ['dont_try']);

In the default mode, BDB file will be created as Btree;

If you specify 'hash', Hash BDB will be created.

If you specify 'dont_try', this module won\'t try to unlock BDB if it detects the situation in which deadlock may occur.

=cut

sub create_write_dbh(){
  my $self=shift;
  my $bdb=File::Spec->rel2abs(shift);
  my $op=shift;
  my $dont_try=shift;
  my $dbh;
  $SIG{ALRM} = sub { die "timeout"};
  eval{
    alarm($self->{'wait'});
    if($op && $op eq 'hash'){
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
        -Env => $self->create_env($bdb)};
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
          return $self->create_write_dbh($bdb, $op, 'dont_try');
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

$self->create_read_dbh($bdb, ['hash'], ['dont_try']);

In the default mode, BDB file will be created as Btree;

If you specify 'hash', Hash BDB will be created.

=cut

sub create_read_dbh(){
  my $self=shift;
  my $bdb=File::Spec->rel2abs(shift);
  my $op=shift;
  my $dbh;
  if($op && $op eq 'hash'){
    $dbh =new BerkeleyDB::Hash {
      -Filename => $bdb,
      -Flags    => DB_RDONLY
      };
  }
  else{
    $dbh =new BerkeleyDB::Btree {
      -Filename => $bdb,
      -Flags    => DB_RDONLY
      };
  }
  return $dbh;
}


=head2

This will creates hash for writing.

$self->create_write_hash_ref($bdb, ['hash'], ['dont_try']);

In the default mode, BDB file will be created as Btree;

If you specify 'hash', Hash BDB will be created.

If you specify 'dont_try', this module won\'t try to unlock BDB if it detects the situation in which deadlock may occur.

=cut

sub create_write_hash_ref(){
  my $self=shift;
  my $bdb=File::Spec->rel2abs(shift);
  my $op=shift;
  my $dont_try=shift;
  my $type='BerkeleyDB::Btree';
  if($op && $op eq 'hash'){
    $type='BerkeleyDB::Hash';
  }
  my %hash;
  
  local $SIG{ALRM} = sub { die "timeout"};
  eval{
    alarm($self->{'wait'});
    tie %hash, $type,
    -Env=>$self->create_env($bdb),
    -Filename => $bdb,
    -Mode => 0666,
    -Flags    => DB_CREATE;
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
          return $self->create_write_has_ref($bdb, $op, 'dont_try');
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

$self->create_read_hash_ref($bdb, ['hash'], ['dont_try']);

In the default mode, BDB file will be created as Btree;

If you specify 'hash', Hash BDB will be created.

If you specify 'dont_try', this module won\'t try to unlock BDB if it detects the situation in which deadlock may occur.

=cut

sub create_read_hash_ref(){
  my $self=shift;
  my $bdb=File::Spec->rel2abs(shift);
  my $op=shift;
  my $type='BerkeleyDB::Btree';
  if($op && $op eq 'hash'){
    $type='BerkeleyDB::Hash';
  }
  my %hash;
  tie %hash, $type,
  -Filename => $bdb,
  -Flags    => DB_RDONLY;
  return \%hash;
}

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
