create schema dom;

create table dom.realty (
    region text,
    city text,
    id integer,
    -- main info
    address text,
    cadastral_id text,
    year text,
    floors text,
    estate_type text,
    rooms text,
    type text,
    playground text,
    sports_ground text,
    company text,
    flooring_type text,
    walls_type text,
    garbage_disposal_type text,
    is_unsafe text,
    space_living text,
    space_common text,
    space text,
    energy_efficient text,
    inputs text,
    gas text,
    sewer text,
    hot_water text,
    cold_water text,
    heating text,
    electricity text,
    primary key (region, city, id)
);
