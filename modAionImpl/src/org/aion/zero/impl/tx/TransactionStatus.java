package org.aion.zero.impl.tx;

import org.aion.base.type.ITransaction;
import org.aion.base.util.ByteUtil;

public class TransactionStatus<T extends ITransaction> {
    public final boolean isRemote;
    public final long timestamp;
    public final long nonce;
    public final T transaction;

    public TransactionStatus(T tx, boolean remote) {
        this.isRemote = remote;
        this.timestamp = System.currentTimeMillis();
        this.nonce = ByteUtil.byteArrayToLong(tx.getNonce());
        this.transaction = tx;
    }
}
