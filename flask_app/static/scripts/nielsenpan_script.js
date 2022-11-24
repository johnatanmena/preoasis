
function nielsenpan_progress_track(par_step){
  if (par_step){
    step = par_step
  }
  step1 = document.getElementById('nielsenpan_step1');
  step2 = document.getElementById('nielsenpan_step2');
  step3 = document.getElementById('nielsenpan_step3');
  step4 = document.getElementById('nielsenpan_step4');
  step5 = document.getElementById('nielsenpan_step5');
  step6 = document.getElementById('nielsenpan_step5');
  if (step === 'step1') {
    step = 'step2';
    step1.classList.remove("is-active");
    step1.classList.add("is-complete");
    step2.classList.add("is-active");

  } else if (step === 'step2') {
    step = 'step3';
    step2.classList.remove("is-active");
    step2.classList.add("is-complete");
    step3.classList.add("is-active");

  } else if (step === 'step3') {
    step = 'step4';
    step3.classList.remove("is-active");
    step3.classList.add("is-complete");
    step4.classList.add("is-active");

  } else if (step === 'step4') {
    step = 'step5';
    step4.classList.remove("is-active");
    step4.classList.add("is-complete");
    step5.classList.add("is-active");

  } else if (step === 'step5') {
    step = 'step6';
    step5.classList.remove("is-active");
    step5.classList.add("is-complete");
  } else if (step === 'step6') {
    step = 'complete';
    step5.classList.remove("is-active");
    step5.classList.add("is-complete");
    step6.classList.add("is-active");
  }else if (step === 'complete') {
    step = 'step1';
    step6.classList.remove("is-complete");
    step5.classList.remove("is-complete");
    step4.classList.remove("is-complete");
    step3.classList.remove("is-complete");
    step2.classList.remove("is-complete");
    step1.classList.remove("is-complete");
    step1.classList.add("is-active");

  }
}

function show_form_nielsenpan(){
	$.ajax({
      url: '/v_nielsen_panama/transform',
      type: "post",
      beforeSend: show_loading(),
      dataType: "html",
      success: function(res){
          $('div#formcontent').html(res); 
          hide_loading();

      }
    });
}

function show_form_assign_data_nielsenpan(category=null){
  if (typeof category === 'string'){
    myurl = '/v_nielsen_panama/form_assign_iv_panama/' + category
  }
  else if (category){
    myurl = '/v_nielsen_panama/form_assign_iv_panama/' + category.value
  }
  else{
    myurl = '/v_nielsen_panama/form_assign_iv_panama'
  }
  $.ajax({
      url: myurl,
      type: "post",
      beforeSend: show_loading(),
      dataType: "html",
      success: function(res){
          $('div#formcontent').html(res); 
          hide_loading();

      }
    });
  
}

/*
Funcion que actualiza un valor de catálogo temporal cuando se presiona el botón actualizar
*/
function actualizar_valor_panama(indice, mst_column){
  new_val = $("#" + indice).val();
  myurl = encodeURI('/v_nielsen_panama/update_value/'+mst_column+'/'+indice+"/"+ new_val)
  $.ajax({
    url: myurl,
    type: "GET",
    beforeSend: function(){
      show_loading();
    },
    dataType: 'JSON',
    headers: {
      'Access-Control-Allow-Origin': '*'
    },
    success: function(res){
      var cod_status = res
      htmlresponse = '<strong>Valor Actualizado</strong> se modifica el catalogo: ' + mst_column +
        ", en la posición "+ indice + ' con el valor <strong>'+ new_val +'</strong>'
      set_result_info_message('divresultexecution', htmlresponse, 'alert-success');
      $('.resultcontent').hide();
    },
    error: function(request, status, error) {
      htmlresponse = '<strong>Mensaje de finalización: </strong>'+ request.responseText
      set_result_info_message('divresultexecution', htmlresponse, 'alert-danger');
      $('.resultcontent').hide();
    },
    complete: function(response_object){
      hide_loading();
      show_form_assign_data_nielsenpan(mst_column)
    }
  })

}


function transform_nielsenpan_data(){
  $('.resultinfo').attr('hidden',true);
  $('.resultcontent').text("");
  
  $.ajax({
    url: 'http://localhost:8000/item_volumen_panama_raw_transform',
    type: "GET",
    dataType: 'JSON',
    contentType: false,
    processData: false,
    beforeSend: function(){
      show_loading();
      $('.resultinfo').attr('hidden',true);
      window.interval_function = setInterval(get_message_log, 1000);

    },
    success: function(res) {                  
      var cod_status = res.cod_status
	    $('#btnassigndefaultvalues').removeAttr('disabled');
	    nielsenpan_progress_track('step1');
      var arr_catalogos = res.catalogos;
      htmlresponse = '<ul><li>' + arr_catalogos.join('</li><li>') + '</li></ul>';
      if (cod_status != 0){
        set_result_info_message('divresultexecution', res.message_response, 'alert-info');
        $('.resultcontent').html(htmlresponse);  
          
      }
      else {
        set_result_info_message('divresultexecution', "Ejecución completada: " + res.message_response, 'alert-info');
        reset_form_content(); 
      }
    },
    complete: function(response_object){
      clearInterval(interval_function);
      seconds = 60;
      get_console_log();
      hide_loading();
      $(".seconds").text("Mostrar Log - Ejecución completada");
    },
    error: function(e) {
       console.error(e);
       set_result_info_message('divresultexecution', "Error de ejecucion: " + e, 'alert-info');
       $.notify("Hubo un error en el proceso de carga y validación del archivo revisar logs", "error");
       hide_loading();
      }
    });
}

function assign_temporary_values_panama(){
  $.ajax({
    url: 'http://localhost:8000/nielsenpan_assign_temporary_values',
    type: "GET",
    beforeSend: function(){
      show_loading();
      $('.resultinfo').attr('hidden',true);
      window.interval_function = setInterval(get_message_log, 1000);
    },
    dataType: 'JSON',
    headers: {
      'Access-Control-Allow-Origin': '*'
    },
    success: function(res){
      $('.divresultexecution').show();
      $('.resultinfo').removeAttr('hidden');
      var cod_status = res.cod_status
      if (cod_status == 0){
        htmlresponse = '<strong>Mensaje de finalización: </strong> Prepararado para iniciar el proceso de carga de Nielsen Panama';
        set_result_info_message('divresultexecution', htmlresponse, 'alert-success');
        $('#btnapplynielsenpanval').removeAttr('disabled');
        nielsenpan_progress_track('step2');
      }else{
        htmlresponse = '<strong>Mensaje de finalización: </strong>'+ res.message_response
        set_result_info_message('divresultexecution', htmlresponse, 'alert-warning');
      }
    },
    error: function(request, status, error) {
      // server error case
      htmlresponse = '<strong>Mensaje de finalización: </strong>'+ request.message_response
      set_result_info_message('divresultexecution', htmlresponse, 'alert-error');
    },
    complete: function(response_object){
      clearInterval(interval_function);
      seconds = 60;
      get_console_log();
      hide_loading();
      $(".seconds").text("Mostrar Log - Ejecución completada");
    }
  })
}

function apply_nielsenpan_validations(){
  reset_form_content();
  apply_validations("iv_panama") // llamar función de carga de Nielsen
}

function publish_nielsenpan_catalogs(){
  reset_form_content();
  publish_catalogs("iv_panama") // llamar publicacion de catalogos de nielsen	
}

function publish_nielsenpan_files(){
  reset_form_content();
  publish_files("iv_panama");
}