/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.concurrent;

import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TPE that handles exceptions (either by logging or by submitting to UncaughtExceptionHandler.uncaughtException()) even
 * in the case where the exception occurs in a FutureTask where get() would have to be called in order to get it.
 */
public class ExceptionHandlingThreadPoolExecutor extends ThreadPoolExecutor
{

    protected static final Logger logger = LoggerFactory.getLogger(ExceptionHandlingThreadPoolExecutor.class);

    public ExceptionHandlingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
            TimeUnit unit, BlockingQueue<Runnable> workQueue)
    {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public ExceptionHandlingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
            TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory)
    {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public ExceptionHandlingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
            TimeUnit unit, BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler)
    {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public ExceptionHandlingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
            TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory,
            RejectedExecutionHandler handler)
    {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t)
    {
        super.afterExecute(r, t);
        logExceptionsAfterExecute(r, t);
    }

    /**
     * Send @param t and any exception wrapped by @param r to the default uncaught exception handler, or log them if
     * none such is set up
     */
    public static void logExceptionsAfterExecute(Runnable r, Throwable t)
    {
        Throwable hiddenThrowable = extractThrowable(r);
        if (hiddenThrowable != null)
            handleOrLog(hiddenThrowable);

        // ThreadPoolExecutor will re-throw exceptions thrown by its Task (which will be seen by
        // the default uncaught exception handler) so we only need to do anything if that handler
        // isn't set up yet.
        if (t != null && Thread.getDefaultUncaughtExceptionHandler() == null)
            handleOrLog(t);
    }

    /**
     * Send @param t to the default uncaught exception handler, or log it if none such is set up
     */
    public static void handleOrLog(Throwable t)
    {
        if (Thread.getDefaultUncaughtExceptionHandler() == null)
            logger.error("Error in ThreadPoolExecutor", t);
        else
            Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
    }

    /**
     * @return any exception wrapped by @param runnable, i.e., if it is a FutureTask
     */
    public static Throwable extractThrowable(Runnable runnable)
    {
        // Check for exceptions wrapped by FutureTask. We do this by calling get(), which will
        // cause it to throw any saved exception.
        //
        // Complicating things, calling get() on a ScheduledFutureTask will block until the task
        // is cancelled. Hence, the extra isDone check beforehand.
        if ((runnable instanceof Future<?>) && ((Future<?>) runnable).isDone())
        {
            try
            {
                ((Future<?>) runnable).get();
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
            catch (CancellationException e)
            {
                logger.debug("Task cancelled", e);
            }
            catch (ExecutionException e)
            {
                return e.getCause();
            }
        }

        return null;
    }
}
