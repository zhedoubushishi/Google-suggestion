// autocomplet : this function will be executed every time we change the text
function autocomplet() {
	var min_length = 1; // min characters to display the autocomplete
	var keyword = $('#input_id').val();
	if (keyword.length >= min_length) {
		$.ajax({
			url: 'ajax_refresh.php',
			type: 'POST',
			data: {keyword:keyword},
			success:function(data){
				$('#output_list_id').show();
				$('#output_list_id').html(data);
			}
		});
	} else {
		$('#output_list_id').hide();
	}
}

// set_item : this function will be executed when we select an item
function set_item(item) {
	// change input value
	$('#input_id').val(item);
	// hide proposition list
	$('#output_list_id').hide();
}