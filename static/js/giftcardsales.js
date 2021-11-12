function openmodel(sel){
	var content = sel.options[sel.selectedIndex].text;
	if (content == "Mark Sold"){

		document.getElementById("con-close-modal").showModal();
		alert(content);
	}
}