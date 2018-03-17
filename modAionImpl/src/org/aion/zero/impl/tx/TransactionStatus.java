package org.aion.zero.impl.tx;

import org.aion.base.type.ITransaction;
import org.aion.base.util.ByteArrayWrapper;
import org.aion.base.util.ByteUtil;

import java.math.BigInteger;

public class TransactionStatus<T extends ITransaction> {
    public final ByteArrayWrapper hash;
    public final boolean isRemote;
    public final long timestamp;
    public final long nonce;
    public final BigInteger value;
    public final T transaction;
    public BigInteger reqBalance;

    public TransactionStatus() {
        hash = null;
        isRemote = false;
        timestamp = 0;
        nonce = 0;
        value = BigInteger.ZERO;
        transaction = null;
    }

    public TransactionStatus(T tx, boolean remote) {
        this.hash = new ByteArrayWrapper(tx.getHash());
        this.isRemote = remote;
        this.timestamp = System.currentTimeMillis();
        this.nonce = ByteUtil.byteArrayToLong(tx.getNonce());
        this.value = ByteUtil.bytesToBigInteger(tx.getValue());
        this.transaction = tx;

        // value + nrg * nrgPrice
        this.reqBalance = this.value.add(BigInteger.valueOf(tx.getNrg()).multiply(BigInteger.valueOf(tx.getNrgPrice())));
    }
}
