package railo.extensions.io.cache.riak;

import railo.loader.engine.CFMLEngine;
import railo.loader.engine.CFMLEngineFactory;
import railo.runtime.exp.PageException;
import railo.runtime.type.Struct;

public class RiakDocument {
	
	private String key;
	private Struct data;
	
	/**
	 * 
	 * @param key
	 */
	public RiakDocument(String key) {
		
		long now = System.currentTimeMillis();
		CFMLEngine engine = CFMLEngineFactory.getInstance();

		this.key = key;
		this.data = engine.getCreationUtil().createStruct();
		
		try{	
			this.data.set("lifeSpan",0);
			this.data.set("timeIdle",0);
			this.data.set("created",now);
			this.data.set("hits",0);
			this.data.set("value",null);
		}catch(PageException e){			
			e.printStackTrace();
		}
				
	}

	/**
	 * Constructor
	 * @param key Document key
	 * @param data 
	 */
	public RiakDocument(String key,Struct data){
		this.key = key;
		this.data = data;
	}
	
	/**
	 * Add a hit to the instance count.
	 */
	public void addHit(){
		try{
			int hits = Integer.parseInt((String)this.data.get("hits"));			
			this.data.set("hits",hits++);
		}catch(PageException e){
			e.printStackTrace();
		}
	}

	/**
	 * Return the Struct containing value and metadata	
	 * @return
	 */
	public Struct getData() {
		return this.data;
	}
	
	/**
	 * Return the document key
	 * @return
	 */
	public String getKey() {
		return this.key;
	}
	
	/**
	 * 
	 * @param lifeSpan
	 */
	public void setLifeSpan(long lifeSpan) {
		try{
			this.data.set("lifeSpan", lifeSpan);			
		}catch(PageException e){
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param idleTime
	 */
	public void setIdleItem(long idleTime) {
		try{
			this.data.set("idleTime", idleTime);			
		}catch(PageException e){
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param created
	 */
	public void setCreated(long created) {
		try{
			this.data.set("created", created);			
		}catch(PageException e){
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param value
	 */
	public void setValue(String value) {
		try{
			this.data.set("value", value);
		}catch(PageException e) {
			e.printStackTrace();
		}
	}
	
}
