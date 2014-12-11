package Dhvani.Test;


import Dhvani.*;
import org.junit.*; 

public class RecommendationTest {
	
	@Test
    public void testingmethod0()
    {
        Recommendation mywrapper = new Recommendation();
        
        String filepath = "E:/Songs/MJ/You Rock My World.mp3";
        try
        {
            mywrapper.RecommendSong(filepath);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        
    }
	
	@Test
    public void testingmethod1()
    {
		Recommendation mywrapper = new Recommendation();
        
        String filepath = "E:/Songs/Eminem/Real Slim Shady.mp3";
        try
        {
            mywrapper.RecommendSong(filepath);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testingmethod2()
    {
    	Recommendation mywrapper = new Recommendation();
        
        String filepath = "E:/Songs/Eminem/Superman.mp3";
        try
        {
            mywrapper.RecommendSong(filepath);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testingmethod3()
    {
    	Recommendation mywrapper = new Recommendation();
        
        String filepath = "E:/Songs/Taylor Swift/love.mp3";
        try
        {
            mywrapper.RecommendSong(filepath);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testingmethod4()
    {
    	Recommendation mywrapper = new Recommendation();
        
        String filepath = "E:/Songs/Taylor Swift/You belong with me.mp3";
        try
        {
            mywrapper.RecommendSong(filepath);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testingmethod5()
    {
        Recommendation mywrapper = new Recommendation();
        
        String filepath = "E:/Songs/MJ/They don't care about us.mp3";
        try
        {
            mywrapper.RecommendSong(filepath);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void testingmethod6()
    {
    	Recommendation mywrapper = new Recommendation();
        
        String filepath = "E:/Songs/MJ/Beat it.mp3";
        try
        {
            mywrapper.RecommendSong(filepath);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }


}
