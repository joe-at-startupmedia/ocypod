//! HTTP handlers for the `/queue` endpoints.

use actix_web::{web, HttpResponse, Responder};
use log::{debug, error};

use crate::application::{RedisManager, file};
use crate::models::{job, queue, ApplicationState, OcyError};

/// Handle `GET /queue` requests to get a JSON list of all existing queues.
///
/// # Returns
///
/// * 200 - JSON response containing list of queue names.
pub async fn index(data: web::Data<ApplicationState>) -> impl Responder {
    let mut conn = data.redis_conn_manager.clone();

    match RedisManager::queue_names(&mut conn).await {
        Ok(queue_names) => HttpResponse::Ok().json(queue_names),
        Err(OcyError::RedisConnection(err)) => {
            error!("Failed to fetch queue names: {}", err);
            HttpResponse::ServiceUnavailable().body(err)
        }
        Err(err) => {
            error!("Failed to fetch queue names: {}", err);
            HttpResponse::InternalServerError().body(err)
        }
    }
}

/// Handles `PUT /queue/{queue_name}` requests.
pub async fn create_or_update(
    path: web::Path<String>,
    json: web::Json<queue::Settings>,
    data: web::Data<ApplicationState>,
) -> impl Responder {
    let queue_name = path.into_inner();
    let queue_settings = json.into_inner();
    let mut conn = data.redis_conn_manager.clone();

    match RedisManager::create_or_update_queue(&mut conn, &queue_name, &queue_settings).await {
        Ok(true) => HttpResponse::Created()
            .header("Location", format!("/queue/{}", queue_name))
            .finish(),
        Ok(false) => HttpResponse::NoContent()
            .reason("Queue setting updated")
            .header("Location", format!("/queue/{}", queue_name))
            .finish(),
        Err(OcyError::BadRequest(msg)) => HttpResponse::BadRequest().body(msg),
        Err(OcyError::RedisConnection(err)) => {
            error!(
                "[queue:{}] failed to create/update queue: {}",
                &queue_name, err
            );
            HttpResponse::ServiceUnavailable().body(err)
        }
        Err(err) => {
            error!(
                "[queue:{}] failed to create/update queue: {}",
                &queue_name, err
            );
            HttpResponse::InternalServerError().body(err)
        }
    }
}

pub async fn delete(path: web::Path<String>, data: web::Data<ApplicationState>) -> impl Responder {
    let queue_name = path.into_inner();
    let mut conn = data.redis_conn_manager.clone();

    match RedisManager::delete_queue(&mut conn, &queue_name).await {
        Ok(true) => HttpResponse::NoContent().reason("Queue deleted").finish(),
        Ok(false) => HttpResponse::NotFound().reason("Queue not found").finish(),
        Err(OcyError::BadRequest(msg)) => HttpResponse::BadRequest().body(msg),
        Err(OcyError::RedisConnection(err)) => {
            error!("[queue:{}] failed to delete queue: {}", &queue_name, err);
            HttpResponse::ServiceUnavailable().body(err)
        }
        Err(err) => {
            error!("[queue:{}] failed to delete queue: {}", &queue_name, err);
            HttpResponse::InternalServerError().body(err)
        }
    }
}

pub async fn settings(
    path: web::Path<String>,
    data: web::Data<ApplicationState>,
) -> impl Responder {
    let queue_name = path.into_inner();
    let mut conn = data.redis_conn_manager.clone();

    match RedisManager::queue_settings(&mut conn, &queue_name).await {
        Ok(summary) => HttpResponse::Ok().json(summary),
        Err(OcyError::NoSuchQueue(_)) => HttpResponse::NotFound().into(),
        Err(OcyError::RedisConnection(err)) => {
            error!(
                "[queue:{}] failed to fetch queue summary: {}",
                &queue_name, err
            );
            HttpResponse::ServiceUnavailable().body(err)
        }
        Err(err) => {
            error!(
                "[queue:{}] failed to fetch queue summary: {}",
                &queue_name, err
            );
            HttpResponse::InternalServerError().body(err)
        }
    }
}

pub async fn size(path: web::Path<String>, data: web::Data<ApplicationState>) -> impl Responder {
    let queue_name = path.into_inner();
    let mut conn = data.redis_conn_manager.clone();

    match RedisManager::queue_size(&mut conn, &queue_name).await {
        Ok(size) => HttpResponse::Ok().json(size),
        Err(OcyError::NoSuchQueue(_)) => HttpResponse::NotFound().into(),
        Err(OcyError::RedisConnection(err)) => {
            error!(
                "[queue:{}] failed to fetch queue size: {}",
                &queue_name, err
            );
            HttpResponse::ServiceUnavailable().body(err)
        }
        Err(err) => {
            error!(
                "[queue:{}] failed to fetch queue size: {}",
                &queue_name, err
            );
            HttpResponse::InternalServerError().body(err)
        }
    }
}

pub async fn job_ids(path: web::Path<String>, data: web::Data<ApplicationState>) -> impl Responder {
    let queue_name = path.into_inner();
    let mut conn = data.redis_conn_manager.clone();

    match RedisManager::queue_job_ids(&mut conn, &queue_name).await {
        Ok(size) => HttpResponse::Ok().json(size),
        Err(OcyError::NoSuchQueue(_)) => HttpResponse::NotFound().into(),
        Err(OcyError::RedisConnection(err)) => {
            error!(
                "[queue:{}] failed to fetch queue size: {}",
                &queue_name, err
            );
            HttpResponse::ServiceUnavailable().body(err)
        }
        Err(err) => {
            error!(
                "[queue:{}] failed to fetch queue size: {}",
                &queue_name, err
            );
            HttpResponse::InternalServerError().body(err)
        }
    }
}

pub async fn create_job(
    path: web::Path<String>,
    json: web::Json<job::CreateRequest>,
    data: web::Data<ApplicationState>,
) -> impl Responder {
    let queue_name = path.into_inner();
    let job_req = json.into_inner();
    let mut conn = data.redis_conn_manager.clone();

    let job_write_res = file::write_job(&queue_name, &job_req).unwrap();

    match RedisManager::create_job(&mut conn, &queue_name, &job_req).await {
        Ok(job_id) => {
            let job_attempt = file::get_job(&queue_name, job_write_res.1);
            debug!("deleting job attempt {:?}", job_attempt);
            let _del = file::delete_job(&queue_name, job_write_res.1);
            HttpResponse::Created()
                .header("Location", format!("/job/{}", job_id))
                .json(job_id)
        },
        Err(OcyError::NoSuchQueue(_)) => {
            HttpResponse::NotFound().reason("Queue Not Found").finish()
        }
        Err(OcyError::BadRequest(msg)) => HttpResponse::BadRequest().body(msg),
        Err(OcyError::RedisConnection(err)) => {
            error!("[queue:{}] failed to create new job: {}", &queue_name, err);
            HttpResponse::ServiceUnavailable().body(err)
        }
        Err(err) => {
            error!("[queue:{}] failed to create new job: {}", &queue_name, err);
            HttpResponse::InternalServerError().body(err)
        }
    }
}

pub async fn next_job(
    path: web::Path<String>,
    data: web::Data<ApplicationState>,
) -> impl Responder {
    let queue_name = path.into_inner();
    let mut conn = data.redis_conn_manager.clone();

    match RedisManager::next_queued_job(&mut conn, &queue_name).await {
        Ok(Some(job)) => HttpResponse::Ok().json(job),
        Ok(None) => match &data.config.server.next_job_delay {
            Some(delay) if !delay.is_zero() => {
                tokio::time::delay_for(delay.0).await;
                HttpResponse::NoContent().into()
            }
            _ => HttpResponse::NoContent().into(),
        },
        Err(OcyError::NoSuchQueue(_)) => HttpResponse::NotFound().into(),
        Err(OcyError::RedisConnection(err)) => {
            error!("[queue:{}] failed to fetch next job: {}", &queue_name, err);
            HttpResponse::ServiceUnavailable().body(err)
        }
        Err(err) => {
            error!("[queue:{}] failed to fetch next job: {}", &queue_name, err);
            HttpResponse::InternalServerError().body(err)
        }
    }
}

pub async fn reattempt_job(
    web::Path((queue_name, timestamp)): web::Path<(String, i64)>,
    data: web::Data<ApplicationState>,
) -> impl Responder {

    let mut conn = data.redis_conn_manager.clone();

    debug!("attempting to reattempt {:?} on {}", timestamp, &queue_name);

    match file::get_job(&queue_name, timestamp) {
        Ok(mut job_req) => {
            debug!("attempting to reattempt {:?} on {}", job_req, timestamp);
            //this will not work in the input value is not an object
            if let Some(serde_json::Value::Object(input)) = &mut job_req.input {
                input.extend([ // note: requires fairly recent (stable) Rust, otherwise arrays are not `IntoIterator`
                    ("attempted_on".to_owned(), timestamp.into()),
                ]);
            }
            match RedisManager::create_job(&mut conn, &queue_name, &job_req).await {
                Ok(job_id) => {
                    debug!("deleting job attempt {:?} on {}", job_req, timestamp);
                    let _del = file::delete_job(&queue_name, timestamp);
                    HttpResponse::Created()
                        .header("Location", format!("/job/{}", job_id))
                        .json(job_id)
                },
                Err(OcyError::NoSuchQueue(_)) => {
                    HttpResponse::NotFound().reason("Queue Not Found").finish()
                }
                Err(OcyError::BadRequest(msg)) => HttpResponse::BadRequest().body(msg),
                Err(OcyError::RedisConnection(err)) => {
                    error!("[queue:{}] failed to reattempt creating new job: {}", &queue_name, err);
                    HttpResponse::ServiceUnavailable().body(err)
                }
                Err(err) => {
                    error!("[queue:{}] failed to reattempt creating new job: {}", &queue_name, err);
                    HttpResponse::InternalServerError().body(err)
                }
            }
        },
        Err(err) => {
            error!("[queue:{}] failed to reattempt failed job creation: {}", &queue_name, err);
            HttpResponse::InternalServerError().body(format!("[queue:{}] failed to reattempt failed job creation: {}", &queue_name, err))
        }
    }
}

