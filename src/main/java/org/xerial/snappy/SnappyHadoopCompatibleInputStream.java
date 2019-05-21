package org.xerial.snappy;

import java.io.IOException;
import java.io.InputStream;


public class SnappyHadoopCompatibleInputStream extends InputStream {

    private final static int INT_LENGTH = 4;

    protected final InputStream input;

    private byte[] currentBuffer;
    private int currentBufferReadPosition = 0;

    public SnappyHadoopCompatibleInputStream(InputStream input) {
        this.input = input;
    }

    @Override
    public int read() throws IOException {
        if (shouldLoadNextBlock() && !nextBlock()) {
            return -1;
        } else {
            int returnValue = currentBuffer[currentBufferReadPosition] & 0xFF;
            currentBufferReadPosition += 1;
            return returnValue;
        }
    }

    private boolean shouldLoadNextBlock() {
        return currentBuffer == null || currentBufferReadPosition + 1 >= currentBuffer.length;
    }

    private boolean nextBlock() throws IOException {
        byte[] newBlock = loadNextBlock();

        if (newBlock == null) {
            return false;
        } else {
            currentBuffer = newBlock;
            currentBufferReadPosition = 0;
            return true;
        }
    }

    private byte[] loadNextBlock() throws IOException {
        Integer uncompressedBlockSize = readNextInt();

        if (uncompressedBlockSize != null) {
            return loadAllSubBlocksInSingleBlock(uncompressedBlockSize);
        } else {
            return null;
        }
    }

    private byte[] loadAllSubBlocksInSingleBlock(int uncompressedBlockSize) throws IOException {
        byte[] uncompressedData = new byte[uncompressedBlockSize];
        int uncompressingIndex = 0;

        while (uncompressingIndex < uncompressedBlockSize) {
            int sizeLoaded = loadSubBlock(uncompressedData, uncompressingIndex);
            uncompressingIndex += sizeLoaded;
        }

        return uncompressedData;
    }

    private int loadSubBlock(byte[] buffer, int position) throws IOException {
        Integer subBlockSize = readNextInt();
        if (subBlockSize == null) {
            throw new IOException("Unexpected end of input stream, one or more sub blocks are missing");
        }
        byte[] compressedSubBlock = readNext(subBlockSize);

        if (compressedSubBlock == null) {
            throw new IOException("Unexpected end of input stream");
        } else {
            return Snappy.rawUncompress(compressedSubBlock, 0, subBlockSize, buffer, position);
        }
    }

    private Integer readNextInt() throws IOException {
        byte[] nextInt = readNext(INT_LENGTH);
        if (nextInt == null) {
            return null;
        } else {
            return SnappyOutputStream.readInt(nextInt, 0);
        }
    }

    private byte[] readNext(int length) throws IOException {
        int read = 0;
        byte[] result = new byte[length];

        while (read < length) {
            int readInSingleGo = input.read(result, read, length - read);
            if (readInSingleGo == -1 && read == 0) {
                return null;
            } else if (readInSingleGo == -1) {
                throw new IOException("Unexpected end of input stream");
            }
            read += readInSingleGo;
        }
        return result;
    }
}
