package hex;

import hex.Layer.ChunksInput;
import hex.Layer.FrameInput;
import hex.Layer.Input;
import hex.NeuralNet.Weights;

import java.io.IOException;
import java.nio.FloatBuffer;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

import water.*;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.util.Log;
import water.util.Utils;

import com.jogamp.opencl.*;
import com.jogamp.opencl.CLMemory.Mem;

/**
 * Trains a neural network.
 */
public abstract class Trainer {
  public Trainer() {
  }

  public abstract Layer[] layers();

  public abstract void run();

  public long steps() {
    throw new UnsupportedOperationException();
  }

  public static class Base extends Trainer {
    final Layer[] _ls;

    public Base(Layer[] ls) {
      _ls = ls;
    }

    @Override public Layer[] layers() {
      return _ls;
    }

    @Override public void run() {
      throw new UnsupportedOperationException();
    }

    final void step() {
      Input input = (Input) _ls[0];
      fprop();

      for( int i = 1; i < _ls.length - 1; i++ )
        Arrays.fill(_ls[i]._e, 0);
      float[] err = _ls[_ls.length - 1]._e;
      int label = input.label();
      for( int i = 0; i < err.length; i++ ) {
        float t = i == label ? 1 : 0;
        err[i] = t - _ls[_ls.length - 1]._a[i];
      }

      bprop();
      input.move();
    }

    final void adjust(long n) {
      for( int i = 1; i < _ls.length; i++ )
        _ls[i].adjust(n);
    }

    final void fprop() {
      for( int i = 0; i < _ls.length; i++ )
        _ls[i].fprop();
    }

    final void bprop() {
      for( int i = _ls.length - 1; i > 0; i-- )
        _ls[i].bprop();
    }
  }

  public static class Direct extends Base {
    int _batch = 20;
    int _batches;
    int _current;

    public Direct(Layer[] ls) {
      super(ls);
    }

    @Override public Layer[] layers() {
      return _ls;
    }

    @Override public void run() {
      for( _current = 0; _batches == 0 || _current < _batches; _current++ ) {
        for( int s = 0; s < _batch; s++ )
          step();
        adjust(_current * _batch);
      }
    }

    @Override public long steps() {
      return _batch * (long) _current;
    }
  }

  /**
   * Runs several trainers in parallel on the same weights. There might be lost updates, but works
   * well in practice. TODO replace by TrainerMR.
   */
  public static class ThreadedTrainers extends Trainer {
    final Base[] _trainers;
    final Thread[] _threads;
    final int _stepsPerThread;
    static final CyclicBarrier DONE = new CyclicBarrier(1);
    volatile CyclicBarrier _suspend;
    final CyclicBarrier _resume;
    final AtomicLong _steps = new AtomicLong();

    public ThreadedTrainers(Layer[] ls) {
      this(ls, 1, 0, 0);
    }

    public ThreadedTrainers(Layer[] ls, int nodes, int index, int steps) {
      _trainers = new Base[Runtime.getRuntime().availableProcessors()];
      _threads = new Thread[_trainers.length];
      _stepsPerThread = steps / _threads.length;
      _resume = new CyclicBarrier(_threads.length + 1);
      for( int t = 0; t < _trainers.length; t++ ) {
        Layer[] clones = new Layer[ls.length];
        for( int i = 0; i < ls.length; i++ )
          clones[i] = Utils.deepClone(ls[i], "_w", "_b", "_in", "_images", "_labels", "_frame", "_caches");
        for( int i = 1; i < ls.length; i++ )
          clones[i]._in = clones[i - 1];
        _trainers[t] = new Base(clones);
        FrameInput input = (FrameInput) _trainers[t]._ls[0];
        int chunks = nodes * _trainers.length;
        input._row = input._frame.numRows() * ((long) index * _trainers.length + t) / chunks;

        final Base trainer = _trainers[t];
        _threads[t] = new Thread("H2O Trainer " + t) {
          @Override public void run() {
            for( int i = 0; _stepsPerThread == 0 || i < _stepsPerThread; i++ ) {
              CyclicBarrier b = _suspend;
              if( b == DONE )
                break;
              if( b != null ) {
                try {
                  b.await();
                  _resume.await();
                } catch( Exception e ) {
                  throw new RuntimeException(e);
                }
              }
              trainer.step();
              _steps.incrementAndGet();
            }
          }
        };
      }
      Log.info("Started " + _trainers.length + " neural network trainers");
    }

    @Override public Layer[] layers() {
      return _trainers[0].layers();
    }

    @Override public void run() {
      start();
      join();
    }

    @Override public long steps() {
      return _steps.get();
    }

    void start() {
      for( int t = 0; t < _threads.length; t++ )
        _threads[t].start();
    }

    void close() {
      _suspend = DONE;
    }

    void suspend() {
      try {
        _suspend = new CyclicBarrier(_threads.length + 1);
        _suspend.await();
        _suspend = null;
      } catch( Exception e ) {
        throw new RuntimeException(e);
      }
    }

    void resume() {
      try {
        _resume.await();
      } catch( Exception e ) {
        throw new RuntimeException(e);
      }
    }

    void join() {
      for( int i = 0; i < _threads.length; i++ ) {
        try {
          _threads[i].join();
        } catch( InterruptedException e ) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  //

  /**
   * Runs ParallelTrainers over all nodes in a cluster, and iteratively merges results.
   */
  public static class Distributed extends Trainer {
    private Layer[] _ls;
    private long _count;
    private long limit;

    public Distributed(Layer[] ls) {
      _ls = ls;
    }

    @Override public long steps() {
      return _count;
    }

    @Override public Layer[] layers() {
      return _ls;
    }

    @Override public void run() {
      int steps = 8192;
      int tasks = (int) (limit / steps);
      for( int task = 0; task < tasks; task++ ) {
        int stepsPerNode = steps / H2O.CLOUD._memary.length;
        Task t = new Task(_ls, task, stepsPerNode);
        Key[] keys = new Key[H2O.CLOUD._memary.length];
        for( int i = 0; i < keys.length; i++ ) {
          String uid = UUID.randomUUID().toString();
          H2ONode node = H2O.CLOUD._memary[i];
          keys[i] = Key.make(uid, (byte) 1, Key.DFJ_INTERNAL_USER, node);
        }
        t.dfork(keys);
        t.join();
        _count += steps;
      }
    }
  }

  static class Task extends DRemoteTask<Task> {
    Layer[] _ls;
    int _index;
    int _steps;
    float[][] _ws, _bs;

    Task(Layer[] ls, int index, int steps) {
      _ls = ls;
      _index = index;
      _steps = steps;
      _ws = new float[_ls.length][];
      _bs = new float[_ls.length][];
      for( int y = 1; y < _ls.length; y++ ) {
        _ws[y] = _ls[y]._w;
        _bs[y] = _ls[y]._b;
      }
    }

    @Override public void lcompute() {
      _ls[0].init(null, _ws[1].length / _bs[1].length);
      for( int y = 1; y < _ls.length; y++ ) {
        _ls[y].init(_ls[y - 1], _bs[y].length);
        System.arraycopy(_ws[y], 0, _ls[y]._w, 0, _ws[y].length);
        System.arraycopy(_bs[y], 0, _ls[y]._b, 0, _bs[y].length);
      }
      int nodes = H2O.CLOUD._memary.length;
      int index = H2O.SELF.index();
      ThreadedTrainers t = new ThreadedTrainers(_ls, nodes, index, _steps);
      t.run();
      // Compute gradient as difference between start and end
      for( int y = 1; y < _ls.length; y++ ) {
        for( int i = 0; i < _ws[y].length; i++ )
          _ws[y][i] = _ls[y]._w[i] - _ws[y][i];
        for( int i = 0; i < _bs[y].length; i++ )
          _bs[y][i] = _ls[y]._b[i] - _bs[y][i];
      }
      tryComplete();
    }

    @Override public void reduce(Task task) {
      for( int y = 1; y < _ls.length; y++ ) {
        for( int i = 0; i < _ws[y].length; i++ )
          _ws[y][i] += task._ws[y][i];
        for( int i = 0; i < _bs[y].length; i++ )
          _bs[y][i] += task._bs[y][i];
      }
    }
  }

  /**
   *
   */
  public static class TrainerMR extends Trainer {
    Layer[] _ls;
    int _epochs;

    public TrainerMR(Layer[] ls, int epochs) {
      _ls = ls;
      _epochs = epochs;
    }

    @Override public Layer[] layers() {
      return _ls;
    }

    @Override public void run() {
      Weights weights = Weights.get(_ls);
      String uid = UUID.randomUUID().toString();
      for( int i = 0; i < H2O.CLOUD._memary.length; i++ ) {
        Key key = key(uid, i);
        UKV.put(key, weights);
      }

      Frame frame = ((FrameInput) _ls[0])._frame;
      assert _ls[0]._a.length == frame._vecs.length - 1;

      for( int i = 0; _epochs == 0 || i < _epochs; i++ ) {
        Pass pass = new Pass();
        pass._uid = uid;
        pass._ls = _ls;
        pass.doAll(frame);
      }

      for( int i = 0; i < H2O.CLOUD._memary.length; i++ ) {
        Key key = key(uid, i);
        UKV.remove(key);
      }
    }

    static Key key(String uid, int node) {
      return Key.make(uid + node, (byte) 1, Key.DFJ_INTERNAL_USER, H2O.CLOUD._memary[node]);
    }
  }

  static class Pass extends MRTask2<Pass> {
    String _uid;
    Layer[] _ls;

    @Override public void map(Chunk[] cs) {
      Layer[] clones = new Layer[_ls.length];
      ChunksInput input = new ChunksInput(cs, true);
      input.init(null, cs.length - 1);
      clones[0] = input;

      Weights weights = UKV.get(TrainerMR.key(_uid, H2O.SELF.index()));
      for( int y = 1; y < _ls.length; y++ ) {
        clones[y] = Utils.newInstance(_ls[y]);
        clones[y].init(clones[y - 1], weights._bs[y].length);
      }
      weights.set(_ls);
      Base base = new Base(clones);
      Log.info("pos:" + cs[0]._start + ", for:" + cs[0]._len);
      for( int i = 0; i < cs[0]._len; i++ ) {
        base.step();
      }
    }

    @Override public void reduce(Pass mrt) {
    }
  }

  /**
   *
   */
  public static class OpenCL extends Trainer {
    final Layer[] _ls;

    public OpenCL(Layer[] ls) {
      _ls = ls;
    }

    @Override public Layer[] layers() {
      return _ls;
    }

    @Override public void run() {
      CLContext context = CLContext.create();
      Log.debug("Created " + context);

      try {
        CLDevice device = context.getMaxFlopsDevice();
        Log.debug("Using " + device);
        CLCommandQueue queue = device.createCommandQueue();

        CLProgram program = context.createProgram(Boot._init.getResource2("/kernels.cl")).build();
        CLKernel[] fprops = new CLKernel[_ls.length];
        CLKernel[] bprops = new CLKernel[_ls.length];
        CLKernel[] resets = new CLKernel[_ls.length];
        CLBuffer<FloatBuffer>[] w = new CLBuffer[_ls.length];
        CLBuffer<FloatBuffer>[] b = new CLBuffer[_ls.length];
        CLBuffer<FloatBuffer>[] a = new CLBuffer[_ls.length];
        CLBuffer<FloatBuffer>[] e = new CLBuffer[_ls.length];
        for( int y = 0; y < _ls.length; y++ ) {
          a[y] = context.createFloatBuffer(_ls[y]._a.length, Mem.READ_WRITE);
          if( y > 0 ) {
            w[y] = context.createFloatBuffer(_ls[y]._w.length, Mem.READ_ONLY);
            b[y] = context.createFloatBuffer(_ls[y]._b.length, Mem.READ_ONLY);
            e[y] = context.createFloatBuffer(_ls[y]._e.length, Mem.READ_ONLY);
            queue.putWriteBuffer(w[y], false);
            queue.putWriteBuffer(b[y], false);

            fprops[y] = program.createCLKernel(_ls.getClass().getSimpleName() + "_fprop");
            fprops[y].putArg(_ls[y - 1]._a.length);
            fprops[y].putArgs(a[y - 1], w[y], b[y], a[y]);

            bprops[y] = program.createCLKernel(_ls.getClass().getSimpleName() + "_bprop");
            bprops[y].putArg(_ls[y - 1]._a.length);
            bprops[y].putArgs(a[y - 1], w[y], b[y], a[y], e[y]);
            bprops[y].putArg(_ls[y]._r);
            if( e[y - 1] != null )
              bprops[y].putArg(e[y - 1]);

            resets[y] = program.createCLKernel("reset_error");
            resets[y].putArg(e[y]);
          }
        }
        int group = device.getMaxWorkGroupSize();
        Input input = (Input) _ls[0];
        for( ;; ) {
          input.fprop();
          for( int i = 0; i < input._a.length; i++ )
            a[0].getBuffer().put(i, input._a[i]);
          queue.putWriteBuffer(a[0], false);
          for( int y = 1; y < fprops.length; y++ )
            queue.put1DRangeKernel(fprops[y], 0, _ls[y]._a.length, group);

          queue.putReadBuffer(a[_ls.length - 1], true);
          for( int y = 1; y < fprops.length - 1; y++ )
            queue.put1DRangeKernel(resets[y], 0, _ls[y]._a.length, group);
          softmax(input, a[a.length - 1].getBuffer(), e[e.length - 1].getBuffer());
          queue.putWriteBuffer(a[_ls.length - 1], false);
          queue.putWriteBuffer(e[_ls.length - 1], false);

          for( int y = _ls.length - 1; y > 0; y-- )
            queue.put1DRangeKernel(bprops[y], 0, _ls[y]._a.length, group);
          input.move();
        }
      } catch( IOException ex ) {
        throw new RuntimeException(ex);
      } finally {
        context.release();
      }
    }

    static void softmax(Input input, FloatBuffer a, FloatBuffer e) {
      float max = Float.NEGATIVE_INFINITY;
      for( int o = 0; o < a.capacity(); o++ )
        if( max < a.get(o) )
          max = a.get(o);
      float scale = 0;
      for( int o = 0; o < a.capacity(); o++ ) {
        a.put(o, (float) Math.exp(a.get(o) - max));
        scale += a.get(o);
      }
      for( int o = 0; o < a.capacity(); o++ ) {
        a.put(o, a.get(o) / scale);
        e.put(o, (o == input.label() ? 1 : 0) - a.get(o));
      }
    }
  }
}
