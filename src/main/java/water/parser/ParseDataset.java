package water.parser;

import java.io.*;
import java.util.*;
import java.util.zip.*;

import sun.security.krb5.internal.PAData;

import jsr166y.CountedCompleter;

import water.*;
import water.DRemoteTask.DFuture;
import water.H2O.H2OCountedCompleter;
import water.api.Inspect;
import water.parser.DParseTask.Pass;

import com.google.common.base.Throwables;
import com.google.common.io.Closeables;

/**
 * Helper class to parse an entire ValueArray data, and produce a structured
 * ValueArray result.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 */
@SuppressWarnings("fallthrough")
public final class ParseDataset extends Job {
  public static enum Compression {
    NONE, ZIP, GZIP
  }

  long _total;
  public final Key _progress;

  private ParseDataset(Key dest, Key[] keys) {
    super("Parse", dest);
    Value dataset = DKV.get(keys[0]);
    long total = dataset.length() * Pass.values().length;
    for (int i = 1; i < keys.length; ++i) {
      dataset = DKV.get(keys[i]);
      total += dataset.length() * Pass.values().length;
    }
    _total = total;
    _progress = Key.make(UUID.randomUUID().toString(), (byte) 0, Key.JOB);
    UKV.put(_progress, new Progress());
  }

  // Guess
  public static Compression guessCompressionMethod(Value dataset) {
    byte[] b = dataset.getFirstBytes(); // First chunk
    AutoBuffer ab = new AutoBuffer(b);
    // Look for ZIP magic
    if( b.length > ZipFile.LOCHDR && ab.get4(0) == ZipFile.LOCSIG )
      return Compression.ZIP;
    if( b.length > 2 && ab.get2(0) == GZIPInputStream.GZIP_MAGIC )
      return Compression.GZIP;
    return Compression.NONE;
  }

  public static void parse(Key outputKey, Key [] keys){
    ParseDataset job =  new ParseDataset(outputKey, keys);
    job.start();
    parse(job,keys, null);
  }
  public static void parse(ParseDataset job, Key [] keys, CsvParser.Setup setup){
    int j = 0;
    Key [] nonEmptyKeys = new Key[keys.length];
    for (int i = 0; i < keys.length; ++i) {
      Value v = DKV.get(keys[i]);
      if (v == null || v.length() > 0) // skip nonzeros
        nonEmptyKeys[j++] = keys[i];
    }
    if (j < nonEmptyKeys.length) // remove the nulls
      keys = Arrays.copyOf(nonEmptyKeys, j);
    if (keys.length == 0) {
      job.cancel();
      return;
    }
    Value v = DKV.get(keys[0]);
    if (setup == null)
      setup = Inspect.csvGuessValue(v);
    else if (setup._data[0] == null) {
      setup = Inspect.csvGuessValue(v, setup._separator);
      assert setup._data[0] != null;
    }
    final int ncolumns = setup._data[0].length;
    Compression compression = guessCompressionMethod(v);
    try {
      UnzipAndParseTask tsk = new UnzipAndParseTask(job, compression, setup);
      tsk.invoke(keys);
      DParseTask [] p2s = new DParseTask[keys.length];
      DFuture [] dfs = new DFuture [keys.length];
      DParseTask phaseTwo = DParseTask.createPassTwo(tsk._tsk);
      // too keep original order of the keys...
      HashMap<Key, FileInfo> fileInfo = new HashMap<Key, FileInfo>();
      long rowCount = 0;
      for(int i = 0; i < tsk._fileInfo.length; ++i)
        fileInfo.put(tsk._fileInfo[i]._ikey,tsk._fileInfo[i]);
      // run pass 2
      for(int i = 0; i < keys.length; ++i){
        FileInfo finfo = fileInfo.get(keys[i]);
        Key k = finfo._okey;
        long nrows = finfo._nrows[finfo._nrows.length-1];
        for(j = 0; j < finfo._nrows.length; ++j)
          finfo._nrows[j] += rowCount;
        rowCount += nrows;
        p2s[i] = new DParseTask(phaseTwo, finfo);
        dfs[i] = p2s[i].fork(k);
      }
      phaseTwo._sigma = new double[ncolumns];
      phaseTwo._invalidValues = new long[ncolumns];
      // now put the results together and create ValueArray header
      for(int i = 0; i < p2s.length; ++i){
        DParseTask t = p2s[i];
        dfs[i].get();
        for (j = 0; j < phaseTwo._ncolumns; ++j) {
          phaseTwo._sigma[j] += t._sigma[j];
          phaseTwo._invalidValues[j] += t._invalidValues[j];
        }
        if ((t._error != null) && !t._error.isEmpty()) {
          System.err.println(phaseTwo._error);
          throw new Exception("The dataset format is not recognized/supported");
        }
        FileInfo finfo = fileInfo.get(keys[i]);
        UKV.remove(finfo._okey);
      }
      phaseTwo.normalizeSigma();
      phaseTwo._colNames = setup._data[0];
      phaseTwo.createValueArrayHeader();
    } catch (Exception e) {
      UKV.put(job.dest(), new Fail(e.getMessage()));
      throw Throwables.propagate(e);
    } finally {
      job.remove();
    }
  }

  public static class ParserFJTask extends H2OCountedCompleter {
    final ParseDataset job;
    Key [] keys;
    CsvParser.Setup setup;

    public ParserFJTask(ParseDataset job, Key [] keys, CsvParser.Setup setup){
      this.job = job;
      this.keys = keys;
      this.setup = setup;
    }
    @Override
    public void compute2() {
      parse(job, keys,setup);
    }
  }

  public static Job forkParseDataset(final Key dest, final Key[] keys, final CsvParser.Setup setup) {
    ParseDataset job = new ParseDataset(dest, keys);
    job.start();
    H2O.submitTask(new ParserFJTask(job, keys, setup));
    return job;
  }

  public static class ParseException extends RuntimeException {
    public ParseException(String msg) {
      super(msg);
    }
  }

  public static class FileInfo extends Iced{
    Key _ikey;
    Key _okey;
    long [] _nrows;
    boolean _header;
  }

  public static class UnzipAndParseTask extends MRTask {
    final ParseDataset _job;
    final Compression _comp;
    DParseTask _tsk;
    FileInfo [] _fileInfo;
    final byte _sep;
    final int _ncolumns;
    final CustomParser.Type _pType;
    final String [] _headers;

    public UnzipAndParseTask(ParseDataset job, Compression comp, CsvParser.Setup setup) {
      _job = job;
      _comp = comp;
      _sep = setup._separator;
      _ncolumns = setup._data[0].length;
      _pType = setup._pType;
      _headers = (setup._header)?setup._data[0]:null;
    }

    @Override
    // Must override not to flatten the keys (which we do not really want to do here)
    public DFuture fork(Key... keys) {
      _keys = keys;
      return dfork();
    }
    @Override
    public void map(final Key key) {
      Key okey = key;
      Value v = DKV.get(key);
      assert v != null;
      if(_comp != Compression.NONE){
        InputStream is = null;;
        try {
          switch(_comp){
          case ZIP:
            ZipInputStream zis = new ZipInputStream(v.openStream());
            ZipEntry ze = zis.getNextEntry();
            // There is at least one entry in zip file and it is not a directory.
            if (ze == null || ze.isDirectory())
              throw new Exception("Unsupported zip file: " + ((ze == null) ? "No entry found": "Files containing directory are not supported."));
            is = zis;
            break;
          case GZIP:
            is = new GZIPInputStream(v.openStream());
            break;
          default:
            throw H2O.unimpl();
          }
          okey = Key.make(new String(key._kb) + "_UNZIPPED");
          ValueArray.readPut(okey, is);
          DKV.write_barrier(); // we have to wait until all the values are available at their target nodes before proceeding with parse!!
          v = DKV.get(okey);
        } catch (Throwable t) {
          System.err.println("failed decompressing data " + key.toString() + " with compression " + _comp);
          UKV.remove(key);
          throw new RuntimeException(t);
        } finally {
         Closeables.closeQuietly(is);
        }
      }
      CsvParser.Setup setup = Inspect.csvGuessValue(v,_sep);
      if(setup._data[0].length != _ncolumns)
        throw new ParseException("Found conflicting number of columns (using separator " + (int)_sep + ") when parsing multiple files. Found " + setup._data[0].length + " columns  in " + key + " , but expected " + _ncolumns);
      boolean header = setup._header;
      if(header && _headers != null) // check if we have the header, it should be the same one as we got from the head
        for(int i = 0; i < setup._data[0].length; ++i)
          header = header && setup._data[0][i].equalsIgnoreCase(_headers[i]);
      setup = new CsvParser.Setup(_sep, header, setup._data, setup._numlines, setup._bits);
      _tsk = DParseTask.createPassOne(v, _job, _pType);
      try {_tsk.passOne(setup);} catch (Exception e) {throw new RuntimeException(e);}
      _fileInfo = new FileInfo[]{new FileInfo()};
      _fileInfo[0]._ikey = key;
      _fileInfo[0]._okey = okey;
      _fileInfo[0]._header = header;
      _fileInfo[0]._nrows = new long[_tsk._nrows.length];
      long numRows = 0;
      for(int i = 0; i < _tsk._nrows.length; ++i){
        numRows += _tsk._nrows[i];
        _fileInfo[0]._nrows[i] = numRows;
      }
    }
    @Override
    public void reduce(DRemoteTask drt) {
      UnzipAndParseTask tsk = (UnzipAndParseTask)drt;
      if(_tsk == null && _fileInfo == null){
        _fileInfo = tsk._fileInfo;
        _tsk = tsk._tsk;
      } else {
        final int n = _fileInfo.length;
        _fileInfo = Arrays.copyOf(_fileInfo, n + tsk._fileInfo.length);
        System.arraycopy(tsk._fileInfo, 0, _fileInfo, n, tsk._fileInfo.length);
        // we do not want to merge nrows from different files, apart from that, we want to use standard reduce!
        tsk._tsk._nrows = _tsk._nrows;
        _tsk.reduce(tsk._tsk);
      }
    }
  }

  // True if the array is all NaNs
  static boolean allNaNs(double ds[]) {
    for (double d : ds)
      if (!Double.isNaN(d))
        return false;
    return true;
  }

  // Progress (TODO count chunks in VA, unify with models?)

  static class Progress extends Iced {
    long _value;
  }

  @Override
  public float progress() {
    if (_total == 0)
      return 0;
    Progress progress = UKV.get(_progress);
    return (progress != null ? progress._value : 0) / (float) _total;
  }

  @Override
  public void remove() {
    DKV.remove(_progress);
    super.remove();
  }
  static final void onProgress(final Key chunk, final Key progress) {
    assert progress != null;
    Value val = DKV.get(chunk);
    if (val == null)
      return;
    final long len = val.length();
    onProgress(len, progress);
  }
  static final void onProgress(final long len, final Key progress) {
    new TAtomic<Progress>() {
      @Override
      public Progress atomic(Progress old) {
        if (old == null)
          return null;
        old._value += len;
        return old;
      }
    }.fork(progress);
  }
}
