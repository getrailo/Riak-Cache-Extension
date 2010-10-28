package railo.extension.io.cache.riak;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.trifork.riak.RiakClient;
import com.trifork.riak.RiakObject;

import railo.commons.io.cache.Cache;
import railo.commons.io.cache.CacheEntry;
import railo.commons.io.cache.CacheEntryFilter;
import railo.commons.io.cache.CacheKeyFilter;
import railo.extension.io.util.Functions;
import railo.loader.engine.CFMLEngine;
import railo.loader.engine.CFMLEngineFactory;
import railo.runtime.config.Config;
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
		
		
		this.rc = new RiakClient(this.host);
				
	}

	public void init(Config config ,String[] cacheName,Struct[] arguments){
		//Not used at the moment
	}
	
	public void init(Config config, String cacheName, Struct arguments) {
		try {
		init(cacheName,arguments);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean contains(String key) {
		FetchResponse resp = this.rc.fetch(this.bucket, key.toLowerCase());
		if(resp.isSuccess()){
			return true;
		}
		return false;
	}

	@Override
	public List entries() {		
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

		FetchResponse resp = this.rc.fetch(this.bucket, key.toLowerCase());
		
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
	public CacheEntry getCacheEntry(String key, CacheEntry arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Struct getCustomInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getValue(String key) throws IOException {
		Object value = getCacheEntry(key.toLowerCase()).getValue();
		if(value == null){
			throw(new IOException("Key [" + key + "] has not been found."));
		}
		return value;
	}

	@Override
	public Object getValue(String key, Object defaultValue) {
		try{
			this.hits++;
			return getValue(key.toLowerCase());			
		}catch (IOException e) {
			this.misses++;
			return defaultValue;
		}
	}

	@Override
	public long hitCount() {
		return this.hits;
	}

	@Override
	public List keys() {
		ArrayList<String> result = new ArrayList<String>();
		BucketResponse r = this.rc.listBucket(this.bucket);
		
		if (r.isSuccess()) {    
			RiakBucketInfo info = r.getBucketInfo();
		    Collection<String> keys = info.getKeys();
		    Iterator it = keys.iterator();
		    while(it.hasNext()){
		    	result.add((String) it.next());
		    }
		    return result;
		}
		return null;
	}

	@Override
	public List keys(CacheKeyFilter filter) {
		ArrayList<String> result = new ArrayList<String>();
		BucketResponse r = this.rc.listBucket(this.bucket);
		String key;
		
		if (r.isSuccess()) {    
			RiakBucketInfo info = r.getBucketInfo();
		    Collection<String> keys = info.getKeys();
		    Iterator it = keys.iterator();
		    while(it.hasNext()){
		    	key = (String) it.next();
		    	if(filter.accept(key)){
		    		result.add(key);
		    	}		    	
		    }
		    return result;
		}
		return null;
	}

	@Override
	public List keys(CacheEntryFilter filter) {
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
				
		RiakDocument doc = new RiakDocument(key.toLowerCase());
		doc.setCreated(created);
		doc.setLifeSpan(lifeSpan);
		doc.setIdleItem(idleTime);
		doc.setValue(val);
		try{
			this.saveDocument(doc);
		}catch(IOException e){
			e.printStackTrace();
		}
				
	}

	@Override
	public boolean remove(String key) {
		HttpResponse resp = this.rc.delete(this.bucket, key);
		if(resp.isSuccess()){
			return true;
		}
		return false;
	}

	@Override
	public int remove(CacheKeyFilter filter) {
		BucketResponse r = this.rc.listBucket(this.bucket);
		String key;
		int counter =0;
		
		if (r.isSuccess()) {    
			RiakBucketInfo info = r.getBucketInfo();
		    Collection<String> keys = info.getKeys();
		    Iterator it = keys.iterator();
		    while(it.hasNext()){
		    	key = (String) it.next();
		    	if(filter.accept(key)){
		    		if(remove(key)) counter++;
		    	}		    	
		    }
		}		
		return counter;
	}

	@Override
	public int remove(CacheEntryFilter filter) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List values() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List values(CacheKeyFilter filter) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List values(CacheEntryFilter filter) {
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
			RiakObject ro = new RiakObject(this.bucket, doc.getKey(),json);
			try{
				this.rc.store(ro);				
			}catch(IOException e){
				throw(new IOException("Cache key [" + doc.getKey() + "] has not been saved."));
			}
			
		}catch(PageException e){
			e.printStackTrace();
		}
	}

}
