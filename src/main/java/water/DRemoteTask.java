package water;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Future;

import jsr166y.CountedCompleter;

import com.google.common.base.Throwables;

/**  A Distributed DTask.
 * Execute a set of Keys on the home for each Key.
 * Limited to doing a map/reduce style.
 */
public abstract class DRemoteTask extends DTask<DRemoteTask> implements Cloneable {
  // Keys to be worked over
  protected Key[] _keys;

  // We can add more things to block on - in case we want a bunch of lazy tasks
  // produced by children to all end before this top-level task ends.
  transient private volatile Futures _fs; // More things to block on

  // Combine results from 'drt' into 'this' DRemoteTask
  abstract public void reduce( DRemoteTask drt );

  // Super-class init on the 1st remote instance of this object.  Caller may
  // choose to clone/fork new instances, but then is reponsible for setting up
  // those instances.
  public void init() { }

  public long memOverheadPerChunk(){return 0;}

  // Invokes the task on all nodes
  public void invokeOnAllNodes() {
    H2O cloud = H2O.CLOUD;
    Key[] args = new Key[cloud.size()];
    String skey = "RunOnAll__"+UUID.randomUUID().toString();
    for( int i = 0; i < args.length; ++i )
      args[i] = Key.make(skey,(byte)0,Key.DFJ_INTERNAL_USER,cloud._memary[i]);
    invoke(args);
    for( Key arg : args ) DKV.remove(arg);
  }

  // Invoked with a set of keys
  public DRemoteTask invoke( Key... keys ) {
    try{fork(keys).get();}catch(Exception e){throw new Error(e);}
  	return this;
  }
  public DRemoteTask fork( Key... keys ) { _keys = flatten(keys); H2O.submitTask(this); return this;}

  private transient boolean _localCompute = false;

  public boolean isLocalCompute(){return _localCompute;}
  protected abstract void localCompute();

  transient RPC<DRemoteTask> _left;
  RPC<DRemoteTask> _rite;
  DRemoteTask _local;
  protected void dCompute(){
    // Split out the keys into disjointly-homed sets of keys.
    // Find the split point.  First find the range of home-indices.
    H2O cloud = H2O.CLOUD;
    int lo=cloud._memary.length, hi=-1;
    for( Key k : _keys ) {
      int i = k.home(cloud);
      if( i<lo ) lo=i;
      if( i>hi ) hi=i;        // lo <= home(keys) <= hi
    }
    // Classic fork/join, but on CPUs.
    // Split into 3 arrays of keys: lo keys, hi keys and self keys
    final ArrayList<Key> locals = new ArrayList<Key>();
    final ArrayList<Key> lokeys = new ArrayList<Key>();
    final ArrayList<Key> hikeys = new ArrayList<Key>();
    int self_idx = cloud.nidx(H2O.SELF);
    int mid = (lo+hi)>>>1;    // Mid-point
    for( Key k : _keys ) {
      int idx = k.home(cloud);
      if( idx == self_idx ) locals.add(k);
      else if( idx < mid )  lokeys.add(k);
      else                  hikeys.add(k);
    }
    // Launch off 2 tasks for the other sets of keys, and get a place-holder
    // for results to block on.
    setPendingCount(Math.min(locals.size(),1) + Math.min(lokeys.size(),1)+Math.min(hikeys.size(),1));
    RPC<DRemoteTask> l = null,r = null;
    if(!lokeys.isEmpty()){
      l = new RPC<DRemoteTask>(lokeys.get(0).home_node(),clone());
      l.setCompleter(this);
      l._dt._keys = lokeys.toArray(new Key[lokeys.size()]);
      l.fork();
    }
    if(!hikeys.isEmpty()){
      r = new RPC<DRemoteTask>(hikeys.get(0).home_node(),clone());
      r.setCompleter(this);
      r._dt._keys = hikeys.toArray(new Key[hikeys.size()]);
      r.fork();
    }
    // Setup for local recursion: just use the local keys.
    if( _keys.length != 0 ) {   // Shortcut for no local work
      _local = clone();
      _local._keys = locals.toArray(new Key[locals.size()]); // Keys, including local keys (if any)
      _local.init();                   // One-time top-level init
      _local._localCompute = true;
      _local.localCompute();
    }
    _left = l;
    _rite = r;
    tryComplete();
  }
  @Override
  public final void compute2() {
  	if(_localCompute) localCompute(); else dCompute();
  }

  public void onLocalCompletion(){}

  private boolean done = false;
  @Override
  public final void onCompletion(CountedCompleter caller){
    assert !done;
    done = true;
    if(_localCompute)
      onLocalCompletion();
    else{
      assert _left == null || _left._done;
      assert _rite == null || _rite._done;
      if(_local != null)reduce(_local);
      if(_left != null)reduce(_left._dt);
      if(_rite != null)reduce(_rite._dt);
    }
  }
  private final Key[] flatten( Key[] args ) {
    if( args.length==1 ) {
      Value val = DKV.get(args[0]);
      // Arraylet: expand into the chunk keys
      if( val != null && val.isArray() ) {
        ValueArray ary = val.get();
        Key[] keys = new Key[(int)ary.chunks()];
        for( int i=0; i<keys.length; i++ )
          keys[i] = ary.getChunkKey(i);
        return keys;
      }
    }
    assert !has_key_of_keys(args);
    return args;
  }

  private boolean has_key_of_keys( Key[] args ) {
    for( Key k : args )
      if( k._kb[0] == Key.KEY_OF_KEYS )
        return true;
    return false;
  }

  public Futures getFutures() {
    if( _fs == null ) synchronized(this) { if( _fs == null ) _fs = new Futures(); }
    return _fs;
  }

  public void alsoBlockFor( Future f ) {
    if( f == null ) return;
    getFutures().add(f);
  }

  public void alsoBlockFor( Futures fs ) {
    if( fs == null ) return;
    getFutures().add(fs);
  }

  protected void reduceAlsoBlock( DRemoteTask drt ) {
    reduce(drt);
    alsoBlockFor(drt._fs);
  }

  // Misc

  public static double[][] merge(double[][] a, double[][] b) {
    double[][] res = new double[a.length + b.length][];
    System.arraycopy(a, 0, res, 0, a.length);
    System.arraycopy(b, 0, res, a.length, b.length);
    return res;
  }

  public static int[] merge(int[] a, int[] b) {
    int[] res = new int[a.length + b.length];
    System.arraycopy(a, 0, res, 0, a.length);
    System.arraycopy(b, 0, res, a.length, b.length);
    return res;
  }

  public static String[] merge(String[] a, String[] b) {
    String[] res = new String[a.length + b.length];
    System.arraycopy(a, 0, res, 0, a.length);
    System.arraycopy(b, 0, res, a.length, b.length);
    return res;
  }

  @Override protected DRemoteTask clone() {
    try {
      DRemoteTask dt = (DRemoteTask)super.clone();
      dt.setCompleter(this); // Set completer, what used to be a final field
      dt._fs = null;         // Clone does not depend on extent futures
      dt.setPendingCount(0); // Volatile write for completer field; reset pending count also
      return dt;
    } catch( CloneNotSupportedException cne ) {
      throw Throwables.propagate(cne);
    }
  }
}
