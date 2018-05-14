package org.aion.db.impl.lmdb;

import static java.nio.ByteBuffer.allocateDirect;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.GetOp.MDB_SET;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.aion.base.util.ByteArrayWrapper;
import org.aion.db.impl.AbstractDB;
import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

public class LMDBWrapper extends AbstractDB {

    private Dbi<ByteBuffer> db;
    private Env<ByteBuffer> env;

    static int maxValueSize = 16 * 1024 * 1024;

    public LMDBWrapper(String name, String path, boolean enableCache, boolean enableCompression) {
        super(name, path, enableCache, enableCompression);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + ":" + propertiesInfo();
    }

    @Override
    public boolean open() {
        if (isOpen()) {
            return true;
        }

        LOG.debug("init database {}", this.toString());

        File f = new File(path);
        File dbRoot = f.getParentFile();

        // make the parent directory if not exists
        if (!dbRoot.exists()) {
            if (!f.getParentFile().mkdirs()) {
                LOG.error("Failed to initialize the database storage for " + this.toString() + ".");
                return false;
            }
        }

        if (!f.exists()) {
            LOG.info("creating directory: {}" + f.getName());

            try{
                f.mkdir();
            }
            catch(SecurityException se){
                LOG.error("Failed to initialize the database storage for " + se);
                return false;
            }

            LOG.info("folder {} created", f.getName());
        }

        env = create().setMapSize(10_485_760).setMaxDbs(1).open(f);

        try {
            db = env.openDbi(name, MDB_CREATE);
        } catch (Exception e1) {
            LOG.error("Failed to open the database " + this.toString() + " due to: ", e1);
            if (e1.getMessage().contains("No space left on device")) {
                LOG.error("Shutdown due to lack of disk space.");
                System.exit(0);
            }

            // close the connection and cleanup if needed
            close();
        }

        return isOpen();
    }

    @Override
    public void close() {
        // do nothing if already closed
        if (db == null) {
            return;
        }

        LOG.info("Closing database " + this.toString());

        try {
            // attempt to close the database
            db.close();
        } catch (Exception e) {
            LOG.error("Failed to close the database " + this.toString() + ".", e);
        } finally {
            // ensuring the db is null after close was called
            db = null;
        }
    }

    @Override
    public boolean isOpen() {
        return db != null;
    }

    @Override
    public boolean isClosed() {
        return !isOpen();
    }

//    @Override
//    public boolean isLocked() {
//        return false;
//    }

    @Override
    public boolean commitCache(Map<ByteArrayWrapper, byte[]> cache) {
        check();

        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            final Cursor<ByteBuffer> c = db.openCursor(txn);
            for (Map.Entry<ByteArrayWrapper, byte[]> e : cache.entrySet()) {
                if (e.getValue() == null) {
                    final ByteBuffer key = allocateDirect(e.getKey().getData().length).put(e.getKey().getData()).flip();
                    if (c.get(key, MDB_SET)) {
                        c.delete();
                    }
                } else {
                    c.put(allocateDirect(e.getKey().getData().length).put(e.getKey().getData()).flip(), allocateDirect(e.getKey().getData().length).put(e.getValue()).flip());
                }
            }

            c.close();
            txn.commit();
            return true;
        } catch (Throwable e) {
            LOG.error("Unable to close commitCache object in " + this.toString() + ".", e.toString());
        }

        return false;
    }


    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public boolean isCreatedOnDisk() {
        return new File(path).exists();
    }

//    @Override
//    public boolean isCreatedOnDisk() {
//        // working heuristic for Ubuntu: both the LOCK and LOG files should get created on creation
//        // TODO: implement a platform independent way to do this
//        return new File(path, "LOCK").exists() && new File(path, "LOG").exists();
//    }

    @Override
    public long approximateSize() {
        check();

        long count = 0;

        File[] files = (new File(path)).listFiles();

        if (files != null) {
            for (File f : files) {
                if (f.isFile()) {
                    count += f.length();
                }
            }
        } else {
            count = -1L;
        }

        return count;
    }

    @Override
    public boolean isEmpty() {
        check();

        try (CursorIterator<ByteBuffer> it = db.iterate(env.txnRead())) {
            return !it.hasNext();
        } catch (Throwable e) {
            LOG.error("isEmpty method error in " + this.toString() + ".", e.toString());
        }

        return true;
    }

    @Override
    public Set<byte[]> keys() {
        Set<byte[]> rtn = new HashSet<>();

        try (final Txn<ByteBuffer> txn = env.txnRead()) {
            final Cursor<ByteBuffer> c = db.openCursor(txn);

            while (c.next()) {
                byte[] arr = new byte[c.key().remaining()];
                c.key().get(arr);
                rtn.add(arr);
            }
        } catch (Throwable e) {
            LOG.error("getKeys throw errors in " + this.toString() + ".", e.toString());
        }

        return rtn;
    }

    @Override
    protected byte[] getInternal(byte[] k) {
        ByteBuffer value = null;
        ByteBuffer key = allocateDirect(env.getMaxKeySize());
        key.put(k).flip();

        try (Txn<ByteBuffer> rtx = env.txnRead()) {
            value = db.get(rtx, key);
        } catch (Throwable e) {
            LOG.error("getInternal throw an error " + this.toString() + ".", e.toString());
        }

        if (value != null) {
            byte[] arr = new byte[value.remaining()];
            value.get(arr);
            return arr;
        } else {
            return null;
        }
    }

    @Override
    public void put(byte[] k, byte[] v) {
        check(k);
        check();

        final ByteBuffer key = allocateDirect(env.getMaxKeySize());
        key.put(k).flip();

        if (v == null) {
            db.delete(key);
        } else {
            final ByteBuffer val = allocateDirect(maxValueSize);
            val.put(v).flip();
            db.put(key, val);
        }
    }

    @Override
    public void delete(byte[] k) {
        check(k);
        check();

        final ByteBuffer key = allocateDirect(env.getMaxKeySize());

        key.put(k).flip();
        db.delete(key);
    }

    @Override
    public void putBatch(Map<byte[], byte[]> inputMap) {
        check(inputMap.keySet());
        check();

        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            final Cursor<ByteBuffer> c = db.openCursor(txn);
            for (Map.Entry<byte[], byte[]> e : inputMap.entrySet()) {
                byte[] k = e.getKey();
                byte[] v = e.getValue();

                if (v == null) {
                    final ByteBuffer key = allocateDirect(e.getKey().length).put(k).flip();
                    if (c.get(key, MDB_SET)) {
                        c.delete();
                    }
                } else {
                    c.put(allocateDirect(e.getKey().length).put(k).flip(), allocateDirect(e.getKey().length).put(v).flip());
                }
            }

            c.close();
            txn.commit();
        } catch (Throwable e) {
            LOG.error("Unable to close putBatch object in " + this.toString() + ".", e.toString());
        }
    }

    private Cursor<ByteBuffer> cursor = null;
    @Override
    public void putToBatch(byte[] key, byte[] value) {
        check(key);
        check();

        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            if (cursor == null) {
                cursor = db.openCursor(txn);
            }

            if (value == null) {
                final ByteBuffer k = allocateDirect(key.length).put(key).flip();
                if (cursor.get(k, MDB_SET)) {
                    cursor.delete();
                }
            } else {
                cursor.put(allocateDirect(key.length).put(key).flip(), allocateDirect(value.length).put(value).flip());
            }

            //TODO: Check need to call close or not.
            cursor.close();

        }  catch (Throwable e) {
            LOG.error("Unable to close putToBatch in " + this.toString() + ".", e.toString());
        }
    }

    @Override
    public void commitBatch() {
        if (cursor != null) {
            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                cursor.close();
                txn.commit();
            }  catch (Throwable e) {
                LOG.error("Unable to close commitBatch in " + this.toString() + ".", e.toString());
            }
        }
    }

    @Override
    public void deleteBatch(Collection<byte[]> keys) {
        check(keys);

        check();

        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            if (cursor == null) {
                cursor = db.openCursor(txn);
            }

            // add delete operations to batch
            // TODO: Considering the parallelstream delete
            for (byte[] key : keys) {
                final ByteBuffer k = allocateDirect(key.length).put(key).flip();
                if (cursor.get(k, MDB_SET)) {
                    cursor.delete();
                }
            }

            //TODO: Check need to call close or not.
            cursor.close();
        }  catch (Throwable e) {
            LOG.error("DeleteBatch throws in " + this.toString() + ".", e.toString());
        }
    }
}