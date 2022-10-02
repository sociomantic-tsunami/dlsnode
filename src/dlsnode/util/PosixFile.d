/*******************************************************************************

    A thin wrapper around POSIX file I/O functionality with convenience
    extensions.

    copyright:
        Copyright (c) 2016-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dlsnode.util.PosixFile;

import ocean.meta.types.Qualifiers : cstring;
import ocean.sys.ErrnoException;

class PosixFile
{
    import fcntl = ocean.stdc.posix.fcntl: open, O_RDWR, O_APPEND, O_CREAT, S_IRUSR, S_IWUSR, S_IRGRP, S_IROTH;
    import core.sys.posix.unistd: write, pwrite, lseek, ftruncate, fdatasync;
    import unistd = core.sys.posix.unistd: close, unlink;
    import core.sys.posix.sys.uio: writev;
    import core.sys.posix.sys.types: off_t, ssize_t;
    import core.stdc.stdio: SEEK_SET;
    import core.stdc.errno: EINTR, errno;

    import ocean.io.FilePath;
    import ocean.util.log.Logger;

    /***************************************************************************

        File name.

    ***************************************************************************/

    public string name;

    /***************************************************************************

        Logger.

    ***************************************************************************/

    public Logger log;

    /***************************************************************************

        File name as NUL terminated C style string.

    ***************************************************************************/

    protected char* namec;

    /***************************************************************************

        Reusable exception.

    ***************************************************************************/

    private FileException e_;

    /***************************************************************************

        File descriptor. A negative value indicats an error opening or creating
        the file.

    ***************************************************************************/

    private int fd_ = -1;


    /***************************************************************************

        Constructor.

        Note: A subclass constructor may call public class methods only after
        this constructor has returned or the class invariant will fail.

        Params:
            dir  = working directory
            name = file name

        Throws:
            FileException on file I/O error.

    ***************************************************************************/

    public this ( cstring dir, cstring name )
    {
        this.log = Log.lookup(name);

        auto path = FilePath(dir);
        path.create();

        auto null_name = FilePath.join(dir, name) ~ '\0';
        this.namec = null_name.dup.ptr;
        this.name = null_name[0 .. $ - 1];

        this.open();
    }


    /***************************************************************************

        Opens the file.

        Note: as this can be called during constructing the object, this
              method is made private so the invariant check woudln't be
              called during constructing the object.

        Throws:
            FileException on file I/O error.

    ***************************************************************************/

    public void open ( )
    {
        this.fd_ = this.restartInterrupted(fcntl.open(this.namec, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH));

        /*
         * Not calling this.enforce() or this.e() at this point, as doing so
         * would call the invariant, which would fail, as this.fd_ < 0.
         */
        if (this.fd_ < 0)
        {
            throw (new FileException(this.name)).useGlobalErrno("unable to open");
        }

        this.log.trace("File opened with file descriptor {}.", this.fd_);
    }


    /***************************************************************************

        Repositions read/write file offset

        Params:
            offset = offset in bytes from whence
            whence = SEEK_SET for absolute position, SEEK_CUR, for
                     position from current location + offset,
                     SEEK_END, for end of the file plus offset bytes

        Returns:
            resulting offset location as measured in bytes from the
            beginning of the file.

    ***************************************************************************/

    public ulong seek ( off_t offset, int whence, string errmsg, string file = __FILE__, long line = __LINE__ )
    out (pos)
    {
        assert(pos <= off_t.max);
    }
    do
    {
        offset = lseek(this.fd_, offset, whence);

        this.enforce(offset >= 0, errmsg, file, line);

        return offset;
    }

    /***************************************************************************

        Truncates the file to be empty.

    ***************************************************************************/

    public void reset ( )
    {
        /*
         * Seek to the beginning because ftruncate() does not change the file
         * position.
         */
        this.seek(0, SEEK_SET, "unable to seek back when resetting");
        this.enforce(this.restartInterrupted(ftruncate(this.fd_, 0)) >= 0, "unable to truncate when resetting");
        this.log.trace("File was reset to position 0");
    }

    /***************************************************************************

        Flushes output buffers using fdatasync().

    ***************************************************************************/

    public void flush ( )
    {
        this.enforce(!fdatasync(this.fd_), "flush: unable to synchronise");
    }

    /***************************************************************************

        Closes the file. Do not call any public method after this method
        returned.

    ***************************************************************************/

    public void close ( )
    {
        this.enforce(!this.restartInterrupted(unistd.close(this.fd_)), "unable to close");
        this.log.info("File closed.");
        this.fd_ = -1;
    }

    /***************************************************************************

        Deletes the file. Do not call any public method after this method
        returned.

    ***************************************************************************/

    public void unlink ( )
    {
        this.enforce(!unistd.unlink(this.namec), "unable to delete");
        this.log.trace("File deleted.");
        this.fd_ = -1;
    }

    /***************************************************************************

        Throws this.e if ok is false/0/null, adding the file name, errno and
        the error message according to errno to the exception message (unless
        errno is 0).

        Params:
            ok   = condition to check
            msg  = exception message
            file = source code file where the condition is mentioned
            line = source code line where the condition is mentioned

        Throws:
            this.e (IOException) if ok is false/0/null.

    ***************************************************************************/

    public void enforce ( T ) ( T ok, string msg, string file = __FILE__, long line = __LINE__ )
    {
        if (!ok)
        {
            throw this.e.useGlobalErrno(msg, file, cast(int)line);
        }
    }

    /***************************************************************************

        Returns the FileException object, creating it if needed.

        Returns:
            the FileException instance.

    ***************************************************************************/

    public FileException e ( )
    {
        if (this.e_ is null)
        {
            this.e_ = new FileException(this.name);
        }

        return this.e_;
    }

    /***************************************************************************

        Reads or writes data from/to the file starting at position pos. Invokes
        op to perform the I/O operation.
        op may not transmit all data with each call and should return the number
        of bytes transmitted or a negative value on error. op is repeatedly
        called until
         - all bytes in data were transmitted or
         - op returned 0; the number of remaining bytes is then returned, or
         - op returned a negative value and set errno a value different to
           EINTR; a FileException is then thrown.

        pos is increased by the number of bytes written, which is data.length -
        the returned value.

        Params:
            data = source or destination buffer to read from or write to, resp.
            pos  = file position, increased by the number of bytes read/written
            op   = I/O function
            errmsg = error message to use if op returns -1
            line = source code line of the call of this method

        Returns:
            the number of bytes n in data that have not been transmitted because
            op returned 0. The remaining bytes are data[$ - n .. $] so n == 0
            indicates that all bytes have been transmitted.

        Throws:
            FileException if op returns a negative value and sets errno to a
            value different to EINTR.

    ***************************************************************************/

    public size_t transmit ( void[] data, ref off_t pos, typeof(&pwrite) op, string errmsg,
                             string file = __FILE__, long line = __LINE__ )
    out (n)
    {
        assert(n <= data.length);
    }
    do
    {
        for (void[] left = data; left.length;)
        {
            if (ssize_t n = this.restartInterrupted(op(this.fd_, data.ptr, data.length, pos)))
            {
                this.enforce(n > 0, errmsg, file, line);
                left = left[n .. $];
                pos += n;
            }
            else // end of file for pread(); pwrite() should
            {    // return 0 iff data.length is 0
                return left.length;
            }
        }

        return 0;
    }

    /***************************************************************************

        Reads or writes data from/to the file at the current position. Invokes
        op to perform the I/O operation.
        op may not transmit all data with each call and should return the number
        of bytes transmitted or a negative value on error. op is repeatedly
        called until
         - all bytes in data were transmitted or
         - op returned 0; the number of remaining bytes is then returned, or
         - op returned a negative value and set errno a value different to
           EINTR; a FileException is then thrown.

        Params:
            data = source or destination buffer to read from or write to, resp.
            op   = I/O function
            errmsg = error message to use if op returns -1
            line = source code line of the call of this method

        Returns:
            the number of bytes n in data that have not been transmitted because
            op returned 0. The remaining bytes are data[$ - n .. $] so n == 0
            indicates that all bytes have been transmitted.

        Throws:
            FileException if op returns a negative value and sets errno to a
            value different to EINTR.

    ***************************************************************************/

    public size_t transmit ( void[] data, typeof(&write) op, string errmsg,
                             string file = __FILE__, long line = __LINE__ )
    out (n)
    {
        assert(n <= data.length);
    }
    do
    {
        for (void[] left = data; left.length;)
        {
            if (ssize_t n = this.restartInterrupted(op(this.fd_, left.ptr, left.length)))
            {
                this.enforce(n > 0, errmsg, file, line);
                left = left[n .. $];
            }
            else // end of file for read(); write() should
            {    // return 0 iff data.length is 0
                return left.length;
            }
        }

        return 0;
    }


    /***************************************************************************

        Returns underlying file descriptor.

        Returns:
            Currently open file descriptor

    ***************************************************************************/

    public int fd ()
    {
        return this.fd_;
    }


    /***************************************************************************

        Executes op, repeating if it yields a negative value and errno is EINTR,
        indicating op was interrupted by a signal.

        Params:
            op = the operation to execute, should report an error by yielding a
                 negative value and setting errno

        Returns:
            the value op yielded on its last execution.

    ***************************************************************************/

    private static T restartInterrupted ( T ) ( lazy T op )
    {
        T x;
        errno = 0;

        do
        {
            x = op;
        }
        while (x < 0 && errno == EINTR);

        return x;
    }
}

/******************************************************************************/

class FileException: ErrnoException
{
    /***************************************************************************

        The name of the file where a failed operation resulted in throwing this
        instance.

    ***************************************************************************/

    public string filename;

    /***************************************************************************

        Constructor.

        Params:
            filename = the name of the file where a failed operation resulted in
                       throwing this instance.

    ***************************************************************************/

    public this ( string filename )
    {
        this.filename = filename;
    }

    /**************************************************************************

        Calls super.set() to render the error message, then prepends
        this.filename ~ " - " to it.

        Params:
            err_num = error number with same value set as in errno
            name = extern function name that is expected to set errno, optional

        Returns:
            this

     **************************************************************************/

    override public typeof(this) set ( int err_num, string name,
                                       string file = __FILE__, int line = __LINE__ )
    {
        super.set(err_num, name, file, line);

        if (this.filename.length)
        {
            this.append(" - ").append(this.filename);
        }

        return this;
    }
}
