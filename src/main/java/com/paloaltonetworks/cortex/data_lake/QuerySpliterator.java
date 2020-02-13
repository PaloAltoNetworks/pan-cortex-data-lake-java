/**
 * QuerySpliterator
 * 
 * Copyright 2015-2020 Palo Alto Networks, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.paloaltonetworks.cortex.data_lake;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import javax.json.JsonValue;

class QuerySpliterator implements Spliterator<JsonValue> {

    private QueryJobResult pageResults;
    private Iterator<JsonValue> pageIterator;
    final QueryIterable qi;
    private final int startPage;
    private int currentPage;
    private int endPage;
    private int remainder;
    private boolean endSignal = false;

    QuerySpliterator(QueryIterable qi) {
        this.qi = qi;
        pageResults = qi.preloadPageResults;
        pageIterator = qi.preloadPageIterator;
        startPage = 0;
        currentPage = 0;
        endPage = (qi.jobId == null) ? 0 : pageResults.rowsInJob / qi.pageSize;
        remainder = (qi.jobId == null) ? 0 : pageResults.rowsInJob % qi.pageSize;
    }

    private QuerySpliterator(QuerySpliterator qs, QueryJobResult preloadPage, int splitPage) {
        this.qi = qs.qi;
        pageResults = preloadPage;
        pageIterator = preloadPage.page.result.data.iterator();
        startPage = splitPage;
        currentPage = splitPage;
        endPage = qs.endPage;
        remainder = qs.remainder;
    }

    private void spliteratorPreLoad()
            throws IllegalArgumentException, IOException, InterruptedException, QueryServiceParseException,
            QueryServiceException, QueryServiceClientException, Http2FetchException, URISyntaxException {
        var pr = qi.lazyInit();
        if (pr != null) { // lazy init created a new job and we must store the first page result.
            pageResults = pr;
            pageIterator = pr.page.result.data.iterator();
            endPage = pr.rowsInJob / qi.pageSize;
            remainder = pr.rowsInJob % qi.pageSize;
        }
        if (!pageIterator.hasNext() && currentPage < endPage) {
            pageResults = qi.loadPage(++currentPage);
            pageIterator = pageResults.page.result.data.iterator();
        }
    }

    @Override
    public boolean tryAdvance(Consumer<? super JsonValue> action) {
        JsonValue jv;
        try {
            spliteratorPreLoad();
            jv = pageIterator.next();
        } catch (Exception e) {
            if (!endSignal) {
                endSignal = true;
                qi.iteratorEnded();
            }
            return false;
        }
        action.accept(jv);
        return true;
    }

    @Override
    public Spliterator<JsonValue> trySplit() {
        try {
            spliteratorPreLoad();
            if (endPage > currentPage) {
                int splitPage = 1 + currentPage + ((endPage - currentPage) >> 1);
                var newSpliterator = new QuerySpliterator(this, qi.loadPage(splitPage), splitPage);
                endPage = splitPage - 1;
                remainder = 0;
                qi.iteratorStarted();
                return newSpliterator;
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public long estimateSize() {
        try {
            spliteratorPreLoad();
            return remainder + qi.pageSize * (endPage - startPage);
        } catch (Exception e) {
            return Long.MAX_VALUE;
        }
    }

    @Override
    public int characteristics() {
        return Spliterator.SIZED | Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.SUBSIZED;
    }
}