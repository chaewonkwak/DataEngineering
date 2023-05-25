package HW2;

public class Movie {
	public String title;
	public double rating;

	public Movie(String _title, double _rating) {
		this.title = _title;
		this.rating = _rating;
	}

	public String toString() {
		return title + " " + rating;
	}
}	
