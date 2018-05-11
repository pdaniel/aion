package org.aion.base.type;

public interface IMsgHeader {

    short getVer();

    byte getCtrl();

    byte getAction();

    int getRoute();

    int getLen();

    void setLen(int _len);

    byte[] encode();
}
