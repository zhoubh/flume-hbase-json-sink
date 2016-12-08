package org.apache.flume.sink.hbase;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
/**
 * Created by zhouzy on 2016/12/8.
 */
public class Json360HbaseEventSerializer implements HbaseEventSerializer {

    private static final String CHARSET_CONFIG = "charset";
    private static final String CHARSET_DEFAULT = "UTF-8";

    protected static final AtomicLong nonce = new AtomicLong(0);

    protected byte[] cf;
    private byte[] payload;
    private Map<String, String> headers;
    private Charset charset;

    @Override
    public void configure(Context context) {
        charset = Charset.forName(context.getString(CHARSET_CONFIG, CHARSET_DEFAULT));
    }

    @Override
    public void configure(ComponentConfiguration conf) {
    }

    @Override
    public void initialize(Event event, byte[] columnFamily) {
        this.headers = event.getHeaders();
        this.payload = event.getBody();
        this.cf = columnFamily;
    }

    protected byte[] getRowKey_2(Calendar cal, String s) {
        String rowKey = String.format("%s%s%s", s, cal.getTimeInMillis(), nonce.getAndIncrement());
        return rowKey.getBytes(charset);
    }

    protected byte[] getRowKey_2(String s) {
        return getRowKey_2(Calendar.getInstance(), s);
    }

    protected byte[] getRowKey_3(String s) {
        return s.getBytes(charset);
    }

    protected byte[] getRowKey(Calendar cal) {
        Long lt = cal.getTimeInMillis();
        String st = lt.toString();
        //String st1 = st.substring(st.length() - 1, st.length());
        String st1 =new StringBuffer(st).reverse().toString()  ;
        String rowKey = String.format("%s%s", st1, nonce.getAndIncrement());
        return rowKey.getBytes(charset);
    }

    protected byte[] getRowKey() {
        return getRowKey(Calendar.getInstance());
    }

    @Override
    public List<Row> getActions() throws FlumeException {
        List<Row> actions = Lists.newArrayList();

        try {

            byte[] rowKey = getRowKey();
            Put put = new Put(rowKey);

            JsonElement element = new JsonParser().parse(new String(payload));

            if (element.isJsonNull()) {
                return actions;
            }

            if (!element.isJsonObject()) {
                return actions;
            }

            JsonObject jsonObject = element.getAsJsonObject();

            Set<Entry<String, JsonElement>> sets = jsonObject.entrySet();
            sets.remove(headers);
// add start by zhoubh. rowkey : lastnumber+send_number + others
            // rowkey 规则调整 改为  logid , 如果msgid为空的话采用随机生成的办法
            for (Entry<String, JsonElement> entry : sets) {
                String xcontent = entry.getValue().toString();
                if (xcontent.startsWith("\"") && xcontent.endsWith("\"")) {
                    xcontent = xcontent.substring(1, xcontent.length() - 1);
                }

/***
 if (entry.getKey().equals("send_number")) {
 String rowKey_1 = xcontent.substring(xcontent.length() - 1, xcontent.length()) + xcontent;
 byte[] rowKey2 = getRowKey_2(rowKey_1);
 put = new Put(rowKey2);
 }
 */


                if (entry.getKey().equals("logid") && xcontent.length() > 0) {
                    byte[] rowKey3 = getRowKey_3(xcontent);
                    put = new Put(rowKey3);
                }
            }
// add end by zhoubh
            for (Entry<String, JsonElement> entry : sets) {
                String xcontent = entry.getValue().toString();
                if (xcontent.startsWith("\"") && xcontent.endsWith("\"")) {
                    xcontent = xcontent.substring(1, xcontent.length() - 1);
                }
                put.addColumn(cf, entry.getKey().getBytes(charset), xcontent.getBytes(charset));

            }

//            for (Map.Entry<String, String> entry : headers.entrySet()) {
//                put.addColumn(cf, entry.getKey().getBytes(charset), entry.getValue().getBytes(charset));
//            }

            actions.add(put);
        } catch (Exception e) {
            throw new FlumeException(e.getMessage(), e);
        }
        return actions;
    }

    @Override
    public List<Increment> getIncrements() {
        return Lists.newArrayList();
    }

    @Override
    public void close() {
    }
}
