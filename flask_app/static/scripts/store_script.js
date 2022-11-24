

/*
Función que se encarga de encapsular la lógica de muestra de un mensaje en un alert de bootstrap
class element para ayudar a la legibilidad del código
*/
function set_result_info_message(class_element, message_html, classname, class_hidden='resultinfo'){
  $('.'+ class_element).show();
  $('.'+ class_element).removeClass('alert-info');
  $('.'+ class_element).removeClass('alert-success');
  $('.'+ class_element).removeClass('alert-danger');
  $('.'+ class_element).removeClass('alert-warning');
  $('.'+ class_element).addClass(classname);
  $('.resultexecution').html(message_html);
  $('.' + class_hidden).removeAttr('hidden');
  $('.resultcontent').show();
}

function storecheck_progress_track(par_step){
  if (par_step){
    step = par_step
  }
  step1 = document.getElementById('storecheck_step1');
  step2 = document.getElementById('storecheck_step2');
  step3 = document.getElementById('storecheck_step3');
  step4 = document.getElementById('storecheck_step4');
  step5 = document.getElementById('storecheck_step5');
  step6 = document.getElementById('storecheck_step6');
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
    step6.classList.add("is-active");
  } else if (step === 'step6') {
    step = 'complete';
    step6.classList.remove("is-active");
    step6.classList.add("is-complete");

  } else if (step === 'complete') {
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


function show_form_raw_data(){
  $.ajax({
      url: '/v_storecheck/transform',
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
  Función que encapsula la lógica de mostrar el formulario de asignar valores 
*/
function show_form_assign_data(category = null){
  if (typeof category === 'string'){
    myurl = '/v_storecheck/assignvalue/' + category
  }
  else if (category){
    myurl = '/v_storecheck/assignvalue/' + category.value
  }
  else{
    myurl = '/v_storecheck/assignvalue'
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
function actualizar_valor(indice, mst_column){
  new_val = $("#" + indice).val();
  myurl = encodeURI('/v_storecheck/update_value/'+mst_column+'/'+indice+"/"+ new_val)
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
      show_form_assign_data(mst_column)
    }
  })

}

function reset_form_content(){
  $('div#formcontent').html("");
}

/*
Funcion encargada de procesar y realizar las validaciones iniciales aun archivo de
Storecheck
*/
function transform_raw_data(){
  $('.resultinfo').attr('hidden',true);
  $('.resultcontent').text("");
  
  $.ajax({
    url: 'http://localhost:8000/storecheck_raw_transform',
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
      $('#btnassigndefaultvalues').removeAttr('disabled')
      storecheck_progress_track('step1');
      var arr_catalogos = res.catalogos;
      if (cod_status != 0){
        htmlresponse = '<ul><li>' + arr_catalogos.join('</li><li>') + '</li></ul>';
        set_result_info_message('divresultexecution', res.message_response, 'alert-warning');
        $('.resultcontent').html(htmlresponse);    
      }
      else {
        set_result_info_message('divresultexecution', "Ejecución completada: " + res.message_response, 'alert-success');
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
       set_result_info_message('divresultexecution', "Error de ejecucion: " + e, 'alert-error');
       $.notify("Hubo un error en el proceso de carga y validación del archivo revisar logs", "error");
       hide_loading();
       clearInterval(interval_function);
       seconds = 60;
       $(".seconds").text("Mostrar Log - Ejecución completada");
      }
    });
  return false;
}

/*
    Función que realiza los cambios de valores detectados en los catálogos temporales
    y los actualiza en la data original (tener en cuenta que no se modificaran los archivos de insumo)
    pero si la versión almacenada en la carpeta temporal
*/
function assign_temporary_values(){
  $.ajax({
    url: 'http://localhost:8000/storecheck_assign_temporary_values',
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
        htmlresponse = '<strong>Mensaje de finalización: </strong> Prepararado para iniciar el proceso de carga de Storecheck';
        set_result_info_message('divresultexecution', htmlresponse, 'alert-success');
        $('#btnapplystorecheckval').removeAttr('disabled');
        storecheck_progress_track('step2');
      }else{
        htmlresponse = '<strong>Mensaje de finalización: </strong>'+ res.message_response
        set_result_info_message('divresultexecution', htmlresponse, 'alert-warning');
      }
    },
    error: function(request, status, error) {
      // server error case
      htmlresponse = '<strong>Mensaje de finalización: </strong>'+ request.message_response
      set_result_info_message('divresultexecution', htmlresponse, 'alert-error');
      clearInterval(interval_function);
      seconds = 60;
      get_console_log();
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

function apply_storecheck_validation(){
  reset_form_content();
  apply_validations("storecheck") // llamar función de carga de Nielsen
}

function publish_storecheck_catalogs(){
  reset_form_content();
  publish_catalogs("storecheck") // llamar publicacion de catalogos de nielsen

}

function publish_storecheck_files(){
  reset_form_content();
  publish_files("storecheck");  // llamar publicacion a ftp de Nielsen
}

