package application;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.layout.BorderPane;
import javafx.scene.paint.Color;
import javafx.stage.Screen;
import javafx.stage.Stage;

public class Interface extends Application {
	private Scene scene;
	@Override
	public void start(Stage primaryStage) {
		try {
			primaryStage.setTitle("Smart Farming");
			double heigthScreen = Screen.getPrimary().getVisualBounds().getHeight();
			double widthScreen = Screen.getPrimary().getVisualBounds().getWidth();
	        scene = new Scene(new Browser(), widthScreen,heigthScreen, Color.web("#666970"));
	        primaryStage.setScene(scene);
	        scene.getStylesheets().add("application.css");        
	        primaryStage.show();
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

}
