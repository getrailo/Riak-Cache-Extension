package railo.extension.io.cache.riak;

import java.io.IOException;
import java.util.List;

import com.basho.riak.client.RiakClient;
import com.basho.riak.client.RiakConfig;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.response.FetchResponse;
import com.basho.riak.client.response.RiakExceptionHandler;
import com.basho.riak.client.response.RiakIORuntimeException;
import com.basho.riak.client.response.RiakResponseRuntimeException;
import com.basho.riak.client.response.StoreResponse;

import railo.commons.io.cache.Cache;
import railo.commons.io.cache.CacheEntry;
import railo.commons.io.cache.CacheEntryFilter;
import railo.commons.io.cache.CacheKeyFilter;
import railo.extension.io.util.Functions;
import railo.loader.engine.CFMLEngine;
import railo.loader.engine.CFMLEngineFactory;
import railo.runtime.exp.PageException;
import railo.runtime.type.Struct;
import railo.runtime.util.Cast;

public class RiakCache implements Cache {
	
	private String cacheName;
	private String host;
	private RiakClient rc;
	private String bucket;
	private Functions func = new Functions();
	
	//counters
	private int hits = 0;
	private int misses = 0;
	
	
	@Override
	public void init(String cacheName, Struct args) throws IOException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast caster = engine.getCastUtil();
		
		this.cacheName = cacheName;
		
		try{
			
			this.host = "http://" + caster.toString(args.get("host")) + "/riak";
			this.bucket = caster.toString(args.get("bucket")); 
				
		}catch (PageException e) {
			e.printStackTrace();
		}
		
		
		RiakConfig config = new RiakConfig(this.host);
		this.rc = new RiakClient(config);
		
		this.rc.setExceptionHandler(new RiakExceptionHandler() {
			
			@Override
			public void handle(RiakResponseRuntimeException arg0) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void handle(RiakIORuntimeException arg0) {
				// TODO Auto-generated method stub
				
			}
		});
		
	}
	
	@Override
	public boolean contains(String arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List entries() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List entries(CacheKeyFilter arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List entries(CacheEntryFilter arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CacheEntry getCacheEntry(String key) throws IOException {
		CFMLEngine engine = CFMLEngineFactory.getInstance();
		Cast caster = engine.getCastUtil();
		Functions func = new Functions();

		FetchResponse resp = this.rc.fetch(this.bucket, key);
		
		if(resp.isSuccess()){
			
			try{
				
				RiakObject ro = resp.getObject();
				Struct data = caster.toStruct(func.deserializeJSON(ro.getValue()));
				return new RiakCacheEntry(new RiakDocument(key,data)); 
					
			}catch(PageException e){
				e.printStackTrace();
			}
			
		}else{
			throw(new IOException("Cache key [" + key + "] could not be fetched from the server. " + resp.getBodyAsString()));
		}
			
		return null;
	}

	@Override
	public CacheEntry getCacheEntry(String arg0, CacheEntry arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Struct getCustomInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getValue(String arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getValue(String arg0, Object arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long hitCount() {
		return this.hits;
	}

	@Override
	public List keys() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List keys(CacheKeyFilter arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List keys(CacheEntryFilter arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long missCount() {
		return this.misses;
	}

	@Override
	public void put(String key, Object value, Long idleTime, Long lifeSpan) {
		Functions func = new Functions();
		String val = "";
		
		long created = System.currentTimeMillis();
		long idle = idleTime==null?0:idleTime.longValue();
		long life = lifeSpan==null?0:lifeSpan.longValue();
		
		try{
			val = func.serialize(value); 
		}catch(PageException e){
			e.printStackTrace();
		}
				
		RiakDocument doc = new RiakDocument(key);
		doc.setCreated(created);
		doc.setLifeSpan(lifeSpan);
		doc.setIdleItem(idleTime);
		doc.setValue(val);
		
	}

	@Override
	public boolean remove(String arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int remove(CacheKeyFilter arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int remove(CacheEntryFilter arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List values() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List values(CacheKeyFilter arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List values(CacheEntryFilter arg0) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	/**
	 * Private method to persist a RiakDocument instance
	 * @param doc
	 */
	private void saveDocument(RiakDocument doc) throws IOException{
		Functions func = new Functions();
		Struct data = doc.getData();
		
		try{
			
			String json = func.serializeJSON(data,false);
			RiakObject ro = new RiakObject(json, doc.getKey());
			ro.setValue(json);
			StoreResponse resp = this.rc.store(ro);
			
			if(!resp.isSuccess()){
				throw(new IOException("Cache key [" + doc.getKey() + "] has not been saved. " + resp.getBodyAsString()));
			}
			
		}catch(PageException e){
			e.printStackTrace();
		}
	}

}
