//! A simple 3D scene with light shining over a cube sitting on a plane.

use bevy::{prelude::*, tasks::TaskPoolBuilder, window::PresentMode};
use bevy::utils::tracing::info_span;

#[derive(Component, Copy, Clone)]
struct Position(Vec3);

#[derive(Component, Copy, Clone)]
struct Rotation(Vec3);

#[derive(Component, Copy, Clone)]
struct Velocity(Vec3);

#[derive(Component, Copy, Clone)]
struct Transformed(Mat4);

fn main() {
    App::new()
        .insert_resource(WindowDescriptor {
            present_mode: PresentMode::Immediate,
            ..default()
        })
        .insert_resource(
            TaskPoolBuilder::new()
                .threads(11)
                .io(|builder| {
                    builder.percent(0.0).min_threads(0).max_threads(12);
                })
                .async_compute(|builder| {
                    builder.percent(0.0).min_threads(0).max_threads(12);
                })
                .compute(|builder| {
                    builder.percent(1.0).min_threads(0).max_threads(12);
                })
                .build(),
        )
        .insert_resource(Msaa { samples: 4 })
        .add_plugins(DefaultPlugins)
        .add_startup_system(setup)
        .add_startup_system(|mut commands: Commands| {
            commands.spawn_batch((0..1000).map(|_| {
                (
                    Transformed(Mat4::from_axis_angle(Vec3::X, 1.2)),
                    Position(Vec3::X),
                    Rotation(Vec3::X),
                    Velocity(Vec3::X),
                )
            }))
        })
        .add_system(|mut query: Query<(&mut Position, &mut Transformed)>| {
            query.par_for_each_mut(10, |(mut pos, mut mat)| {
                let _guard = info_span!("par closure").entered();

                for _ in 0..100 {
                    mat.0 = mat.0.inverse();
                }

                pos.0 = mat.0.transform_vector3(pos.0);
            });
        })
        .run();
}

/// set up a simple 3D scene
fn setup(
    mut commands: Commands,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<StandardMaterial>>,
) {
    // plane
    commands.spawn_bundle(PbrBundle {
        mesh: meshes.add(Mesh::from(shape::Plane { size: 5.0 })),
        material: materials.add(Color::rgb(0.3, 0.5, 0.3).into()),
        ..default()
    });
    // cube
    commands.spawn_bundle(PbrBundle {
        mesh: meshes.add(Mesh::from(shape::Cube { size: 1.0 })),
        material: materials.add(Color::rgb(0.8, 0.7, 0.6).into()),
        transform: Transform::from_xyz(0.0, 0.5, 0.0),
        ..default()
    });
    // light
    commands.spawn_bundle(PointLightBundle {
        point_light: PointLight {
            intensity: 1500.0,
            shadows_enabled: true,
            ..default()
        },
        transform: Transform::from_xyz(4.0, 8.0, 4.0),
        ..default()
    });
    // camera
    commands.spawn_bundle(PerspectiveCameraBundle {
        transform: Transform::from_xyz(-2.0, 2.5, 5.0).looking_at(Vec3::ZERO, Vec3::Y),
        ..default()
    });
}
