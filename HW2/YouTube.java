package HW2;

public class YouTube {
	public String category;
	public double rating;

	public YouTube(String _category, double _rating) {
		this.category = _category;
		this.rating = _rating;
	}

	public String toString() {
		return category + " " + rating;
	}
}	
