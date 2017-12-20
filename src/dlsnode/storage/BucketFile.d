/******************************************************************************

    Versioned bucket file. It wraps a functionality to have a
    version header and different read/write behaviour depending
    on the bucket version.

    copyright:
        Copyright (c) 2016-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.storage.BucketFile;

import ocean.transition;

import ocean.io.model.IConduit;
import ocean.transition;
import dlsnode.util.aio.AsyncIO;
import dlsnode.util.aio.ContextAwaitingJob;

import dlsnode.storage.protocol.model.IStorageProtocol;
import dlsnode.storage.protocol.StorageProtocolLegacy;
import dlsnode.storage.protocol.StorageProtocolV1;

import ocean.util.serialize.contiguous.package_;

/*******************************************************************************

    Wrapper around File which provides versioning.

    The first 4 bytes will be interpreted as a bucket version if the value of it
    is less or equal than the currently supported version. Otherwise, the legacy
    version is assumed.

    In case the bucket is open for writing and the position is 0, the version
    header will be written. In cases of opening an existing file, either for
    reading or for appending, the existing version will be read.

*******************************************************************************/

public class BucketFile: OutputStream
{
    import dlsnode.storage.Record;
    import ocean.core.Buffer;
    import ocean.core.Enforce;
    import ocean.io.device.File;
    import ocean.io.serialize.SimpleStreamSerializer;
    import ocean.sys.ErrnoException;

    import DlsInputBuffer = dlsnode.storage.util.InputBuffer;

    // TODO; remove with ocean v2.5.0
    import core.stdc.errno;
    import core.sys.posix.sys.types: off_t;
    import posix = core.sys.posix.unistd;

    /**************************************************************************

        Bucket header. Represents a binary format version.

    **************************************************************************/

    private align (1) static struct BucketHeader
    {
        ulong version_no;
        ubyte[8] magic;
    }

    /**************************************************************************

        Magic value

        To ensure that writers can choose to write values lower than
        current_version of bucket file, there is now a DLSBUCKT stamp
        in the place where the length of the record would be. As it is
        very large value, 6x10^18 bytes, it's not possible to physically
        store it anyway in an underlying storage, so it's safer to asume
        that it will be good indicator of magic header.

    **************************************************************************/

    const ubyte[8] magic = ['D', 'L', 'S', 'B', 'U', 'C', 'K', 'T'];

    /**************************************************************************

        Highest supported bucket version.

    **************************************************************************/

    private const ulong current_version = 1;


    /**************************************************************************

        Size of the bucket header.

    *************************************************************************/

    public const BucketHeaderSize = BucketHeader.sizeof;

    /*************************************************************************

        Legacy (non-versioned) bucket version

    **************************************************************************/

    private const ulong legacy_version = ulong.max;


    // Make sure we don't overflow with version numbers.
    static assert (current_version != legacy_version);

    /**************************************************************************

        File instance this object wraps.

    **************************************************************************/

    private File file;


    /*************************************************************************

        AsyncIO instance.

    *************************************************************************/

    private AsyncIO async_io;


    /**************************************************************************

        Current file position.

    **************************************************************************/

    private size_t file_pos_;


    /**************************************************************************

        File length.

    **************************************************************************/

    private size_t file_length_;

    /***************************************************************************

        Indicator if the file is open.

    ***************************************************************************/

    private bool is_open_;


    /**************************************************************************

        Bucket file version.

    **************************************************************************/

    private ulong bucket_version;


    /**************************************************************************

        Constructor.

        Params:
            async_io = AsyncIO instance

    **************************************************************************/

    public this ( AsyncIO async_io )
    {
        this.file = new File;
        this.async_io = async_io;
    }

    /**************************************************************************

        InputBuffer for this file. Used to read underlying file in bigger
        chunks.

    **************************************************************************/

    private DlsInputBuffer.InputBuffer buffered_input;

    /**************************************************************************

        Current cursor position inside a file.

        Returns:
            current cursor position inside a file.

    **************************************************************************/

    public size_t file_pos()
    {
        return this.file_pos_ - this.buffered_input.remainingInBuffer();
    }


    /**************************************************************************

        File length.

        Returns:
            file length

    **************************************************************************/

    public size_t file_length()
    {
        return this.file_length_;
    }


    /**************************************************************************

        File path.

        Returns:
            file path

    **************************************************************************/

    public istring file_path ()
    {
        return this.file.toString();
    }

    /***************************************************************************

        Returns:
            indicator if the file is open.

    ***************************************************************************/

    public bool is_open ()
    {
        return this.is_open_;
    }

    /**************************************************************************

        Creates an instance and opens the bucket file and reads or writes the
        bucket header.

        Params:
            async_io = AsyncIO instance
            waiting_context = ContextAwaitingJob to block
                the fiber on until read is completed. When `null`, `close` will
                block the entire thread.
            path = file path
            file_buffer = buffer used for buffered input
            style = style in which file will be open for

    **************************************************************************/

    public this ( AsyncIO async_io,
            ContextAwaitingJob waiting_context,
            cstring path, void[] file_buffer,
            File.Style style = File.ReadExisting)
    {
        this(async_io);
        this.open(path, waiting_context, file_buffer, style);
    }

    /**************************************************************************

        Opens the bucket file and reads or writes the bucket header.
        This file might be opened with a different iterator each time, which
        would own the different `file_buffer` instances, so it needs to be
        reset here.

        Params:
            path = file path
            waiting_context = ContextAwaitingJob to block
                the fiber on until read is completed
            file_buffer = buffer used for buffered input
            style = style in which file will be open for

    **************************************************************************/

    public void open (cstring path,
            ContextAwaitingJob waiting_context,
            void[] file_buffer,
            File.Style style = File.ReadExisting)
    in
    {
        assert(style == File.ReadExisting || style == File.ReadWriteAppending,
                "Currently BucketFile only supports ReadExisting or ReadWriteAppending");
    }
    body
    {
        this.buffered_input.reset(file_buffer);
        this.file.open(path, style);
        this.file_length_ = this.file.length;
        this.file_pos_ = this.file.position;
        this.is_open_ = true;

        // In case we opened this file for writting,
        // but the position is at 0 (empty file), we need to
        // write the magic header.
        if (style == File.ReadWriteAppending && this.file_length_ == 0)
        {
            this.writeBucketHeader();
        }
        else
        {
            // in case we're openning already existing file,
            // just read the existing header (if any).
            // NOTE: there's no need to reposition cursor. The reason is that
            // we have opened underlying file with O_APPEND and writes will
            // always be appended to the end of the file
            this.readBucketHeader(waiting_context);
        }
    }

    /**************************************************************************

        Closes the file.

        Params:
            waiting_context = ContextAwaitingJob to block
                the fiber on until read is completed. When `null`, `close` will
                block entire thread.

    **************************************************************************/

    public void close (ContextAwaitingJob waiting_context)
    {
        if (waiting_context is null)
        {
            this.file.close();
        }
        else
        {
            this.async_io.close(this.file.fileHandle(), waiting_context);
        }

        this.file_pos_ = 0;
        this.file_length_ = 0;
        this.is_open_ = false;
        this.bucket_version = this.legacy_version;
    }

    /**************************************************************************

        Closes the file. Implements OutputStream.close

        Note:
            this method is calling `close` in a blocking fashion,
            so it can block entire thread.

    **************************************************************************/

    public override void close ( )
    {
        this.close(null);
    }


    /**************************************************************************

        Changes the cursor position inside the file.

        Params:
            offset = offset from the anchor to move the cursor
            anchor = file anchor to measure offset from

        Returns:
            new position within a file

    **************************************************************************/

    public long seek (long offset, File.Anchor anchor = File.Anchor.Begin)
    {
        this.file_pos_ = this.buffered_input.seek(offset, anchor, &this.file.seek);
        return this.file_pos_;
    }


    /**************************************************************************

        Reads and deserializes the data from the file.

        Params:
            waiting_context = ContextAwaitingJob to block
                the fiber on until read is completed.
                If null, this call will perform a blocking read request,
                not switching to any other fiber, while data is being read.
            buf = a buffer to write the data read from the file

        Returns:
            number of bytes read from the file

    **************************************************************************/

    public size_t readData (ContextAwaitingJob waiting_context,
            void[] buf)
    {
        size_t bytes_read;

        if (waiting_context is null)
        {
            bytes_read = this.buffered_input.readData(buf,
                 delegate (void[] buf) {
                     long read_from_file = this.pread(buf,
                             this.file_pos_);

                     this.file_pos_ += read_from_file;
                     return read_from_file;
            });
        }
        else
        {
            assert(this.async_io);

            bytes_read = this.buffered_input.readData(buf,
                 delegate (void[] buf) {
                     long read_from_file = this.async_io.pread(buf,
                             this.file.fileHandle,
                             this.file_pos_, waiting_context);

                     this.file_pos_ += read_from_file;
                     return read_from_file;
            });
        }

        enforce(bytes_read == buf.length, typeof(this).stringof ~ "readData failed");
        return bytes_read;

    }

    /**************************************************************************

        Reads the bucket header.

        Params:
            waiting_context = ContextAwaitingJob to block
                the fiber on until read is completed

    **************************************************************************/

    private void readBucketHeader(ContextAwaitingJob waiting_context)
    {
        BucketHeader header;

        // in case file is open for appending, the cursor will be at
        // the end of file. We need to seek to the beginning of the file,
        // read the version, read record and move back to the end.

        auto previous_pos = this.file_pos_;

        // rewind to the beginning of the file
        this.seek(0);

        size_t bytes_read;

        // Read header of next record
        assert(this.async_io);

        ubyte[header.sizeof] buf;
        this.readData(waiting_context, buf);

        // Async reads do not seek the file position, we need to do
        // manual intervention if needed.
        this.seek(header.sizeof);

        void[] void_buf = cast(void[])buf;
        header = *Deserializer.deserialize!(BucketHeader)(void_buf).ptr;

        if (header.magic[] == this.magic[] && header.version_no <= this.current_version)
        {
            // Versioned bucket, save the version
            this.bucket_version = header.version_no;

            // In this case, we'll seek only to the end of the file if
            // cursor was not positioned to the beginning of the file.
            if (previous_pos > 0)
            {
                this.seek(previous_pos);
            }
        }
        else
        {
            // No version, we need to seek to the beginning
            this.bucket_version = legacy_version;

            // Seek either to the beginning of the file (if previous_pos is 0),
            // in case we open file for reading but there's no bucket header,
            // or to the end of the file in case we have open file for
            // ReadWriteAppending
            this.seek(previous_pos);
        }
    }

    /***************************************************************************

        Writes the bucket header.

    ***************************************************************************/

    private void writeBucketHeader()
    {
        this.bucket_version = this.current_version;

        BucketHeader header;
        header.version_no = this.bucket_version;
        header.magic[] = this.magic[];

        size_t bytes_written;

        bytes_written = SimpleStreamSerializer.writeData(this.file,
                &header, header.sizeof);

        enforce(bytes_written == header.sizeof,
            typeof(this).stringof ~ "writeBucketVersion - header write failed");
    }

    /***************************************************************************

        Delegates the work to the right (scoped) instance of IStorageProtocol
        depending on the bucket version.

        Params:
            dg = delegate to call with the appropriate instance IStorageProtocol.

        Returns:
            return value of `dg`

    ****************************************************************************/

    public T performFileLayoutStrategy(T) ( T delegate ( IStorageProtocol ) dg )
    {
        static if (is(T == void))
        {
            switch (this.bucket_version)
            {
                case 1:
                    scope protocol = new StorageProtocolV1;
                    dg(protocol);
                    break;

                default:
                    scope protocol = new StorageProtocolLegacy;
                    dg(protocol);
                    break;
            }
        }
        else
        {
            switch (this.bucket_version)
            {
                case 1:
                    scope protocol = new StorageProtocolV1;
                    return dg(protocol);

                default:
                    scope protocol = new StorageProtocolLegacy;
                    return dg(protocol);
            }
        }
    }

    /***************************************************************************

        Reads the header of a record from the current seek position of an open
        bucket file. The seek position is moved ready to read the record's
        value.

        Params:
            waiting_context = ContextAwaitingJob to block
                the fiber on until read is completed
            header = (output) header of next record

        Returns:
            true if the end of the bucket file was reached

    ***************************************************************************/

    public bool nextRecord ( ContextAwaitingJob waiting_context,
            ref RecordHeader header )
    {
        return this.performFileLayoutStrategy((IStorageProtocol strategy)
                {
                    return strategy.nextRecord(waiting_context, this, header);
                });
    }

    /**************************************************************************

        Appends a record to the output buffer.

        Params:
            output = BufferedOutput instance wrapping this file
            key = record key
            value = record value
            record_buffer = buffer used internally for rendering entire record
                            passing it to BufferedOutput.

     **************************************************************************/

    public void writeRecord ( BufferedOutput output, hash_t key, cstring value,
            ref ubyte[] record_buffer )
    {
        this.performFileLayoutStrategy((IStorageProtocol strategy)
            {
                strategy.writeRecord(output, key, value, record_buffer);
            });
    }

    /***************************************************************************

        Reads the value of a record from the current seek position of an open
        bucket file. The seek position is moved ready to read the next record's
        header.

        Params:
            waiting_context = ContextAwaitingJob to block
                the fiber on until read is completed
            header = header of current record
            value = (output) receives record value

    ***************************************************************************/

    public void readRecordValue ( ContextAwaitingJob waiting_context,
        RecordHeader header,
        ref mstring value )
    {
        this.performFileLayoutStrategy((IStorageProtocol strategy)
               {
                    strategy.readRecordValue(waiting_context, this, header, value);
               });
    }

    /***************************************************************************

        Skips over the value of a record from the current seek position of an
        open bucket file, without actually reading the value from the file. The
        seek position is moved ready to read the next record's header.

        Params:
            waiting_context = ContextAwaitingJob to block
                the fiber on until read is completed
            header = header of current record

    ***************************************************************************/

    public void skipRecordValue (
            ContextAwaitingJob waiting_context,
            RecordHeader header )
    {
        this.performFileLayoutStrategy((IStorageProtocol strategy)
               {
                    strategy.skipRecordValue(waiting_context, this, header);
               });
    }

    /***********************************************************************

        Write to stream from a source array. The provided src
        content will be written to the stream.
        Returns the number of bytes written from src, which may
        be less than the quantity provided. Eof is returned when
        an end-of-flow condition arises.

        Params:
            src = record to write to the file

    ***********************************************************************/

    public size_t write (Const!(void)[] src)
    {
        return this.file.write(src);
    }

    /***********************************************************************

        Transfer the content of another stream to this one. Returns
        a reference to this class, and throws IOException on failure.

    ***********************************************************************/

    public OutputStream copy (InputStream src, size_t max = -1)
    {
        return this.file.copy(src, max);
    }

    /***********************************************************************

        Return the upstream sink.

    ***********************************************************************/

    public OutputStream output ()
    {
        return this.file.output();
    }

    /***********************************************************************

        Return the host conduit.

    ***********************************************************************/

    public IConduit conduit ()
    {
        return this.file.conduit;
    }

    /***********************************************************************

        Flush buffered content. For InputStream this is equivalent
        to clearing buffered content.

    ***********************************************************************/

    public IOStream flush ()
    {
        return this.file.flush;
    }

    /**************************************************************************

        Calls fsync on the underlying file descriptor.

        Params:
            waiting_context = ContextAwaitingJob instance to
                block the current fiber on. If null,
            the entire thread will be blocked.

    **************************************************************************/

    public void sync (ContextAwaitingJob waiting_context)
    {
        if (waiting_context)
        {
            this.async_io.fsync(this.file.fileHandle, waiting_context);
        }
        else
        {
            this.file.sync;
        }
    }

    /**************************************************************************

        Queries the underlying stream for the current file position. This
        is needed as we can't reliably track writers for the write position,
        as it is being incremented only on flush.

        Returns:
            offset of the beginning of the file where the cursor is.

    **************************************************************************/

    public size_t noncached_file_pos ()
    {
        return this.file.position;
    }

    /***************************************************************

            Read a chunk of bytes from the file into the provided
            array. Returns the number of bytes read, or Eof where
            there is no further data.

            TODO: remove with ocean 2.5.0

    ***************************************************************/

    private size_t pread (void[] dst, off_t offset)
    {
            auto read = posix.pread (this.file.fileHandle(), dst.ptr, dst.length,
                    offset);

            if (read is -1)
                this.file.error("pread");
            else
               if (read is 0 && dst.length > 0)
                   return Eof;
            return read;
    }
}
