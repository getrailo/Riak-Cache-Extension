package railo.extensions.io.cache.riak;

import java.io.IOException;

import com.basho.riak.client.RiakBucketInfo;
import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.response.BucketResponse;
import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.StoreResponse;

public class Runner {
	
	public static void main(String[] args) {
		
		try{
			RiakClient rc = new RiakCache().init("test");
			
			BucketResponse br = rc.listBucket("bucket");
			
			RiakObject o = new RiakObject("bucket","key");
			o.setValue("value");
			StoreResponse sr = rc.store(o);
			System.out.println(sr.isSuccess());
			
			FetchResponse fr = rc.fetch("bucket", "key");
			System.out.println(fr.getObject().getValue());
			
		}catch(IOException e){
			e.printStackTrace();
		}
		
	}

}
