// при выборе строчки в id_tips кидаем выбранное значение в id_rule
window.onload=function() {
document.getElementById("id_tips").onchange=function(){
document.getElementById("id_rule").value=this.value
}}