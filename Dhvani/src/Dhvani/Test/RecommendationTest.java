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


}
