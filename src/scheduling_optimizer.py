"""
Optimal Upload Scheduling System
Phase 3 Enhancement - Smart timing algorithms for maximum engagement
"""

import logging
from datetime import datetime, timedelta, time
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import pytz
from dateutil import parser

from .config import Config
from .database import Database

logger = logging.getLogger(__name__)

@dataclass
class ScheduleSlot:
    """Represents an optimal upload time slot"""
    datetime: datetime
    priority: int  # 1 = highest, 5 = lowest
    slot_type: str  # 'prime', 'good', 'standard'
    reason: str    # Why this slot was chosen
    content_type: str  # 'vod', 'short', 'clip'

class SchedulingOptimizer:
    def __init__(self):
        """Initialize the scheduling optimizer"""
        self.config = Config()
        self.db = Database()
        
        # Default timezone for Sir_Kris (assuming EST/EDT)
        self.timezone = pytz.timezone('US/Eastern')
        
        # Optimal time windows for different content types
        # Based on general YouTube engagement patterns
        self.prime_times = {
            'shorts': [
                (time(7, 0), time(9, 0)),   # Morning commute
                (time(12, 0), time(14, 0)), # Lunch break
                (time(17, 0), time(20, 0)), # Evening commute + prime time
                (time(21, 0), time(23, 0))  # Late evening
            ],
            'vod': [
                (time(2, 0), time(6, 0)),   # Overnight (less competition)
                (time(14, 0), time(16, 0)), # Afternoon
                (time(20, 0), time(22, 0))  # Prime time
            ]
        }
        
        # Day preferences (0 = Monday, 6 = Sunday)
        self.day_priorities = {
            'shorts': {
                0: 3,  # Monday - good
                1: 2,  # Tuesday - better  
                2: 2,  # Wednesday - better
                3: 1,  # Thursday - best
                4: 1,  # Friday - best
                5: 3,  # Saturday - good
                6: 4   # Sunday - standard
            },
            'vod': {
                0: 2,  # Monday - better
                1: 1,  # Tuesday - best
                2: 1,  # Wednesday - best
                3: 1,  # Thursday - best
                4: 2,  # Friday - better
                5: 3,  # Saturday - good
                6: 3   # Sunday - good
            }
        }
        
        logger.info("Scheduling optimizer initialized")
    
    def get_optimal_vod_time(self, stream_ended_at: datetime) -> datetime:
        """
        Get optimal upload time for VOD (typically overnight after stream ends)
        
        Args:
            stream_ended_at: When the stream ended
            
        Returns:
            Optimal datetime for VOD upload
        """
        # Ensure timezone awareness
        if stream_ended_at.tzinfo is None:
            stream_ended_at = self.timezone.localize(stream_ended_at)
        else:
            stream_ended_at = stream_ended_at.astimezone(self.timezone)
        
        # Calculate overnight window (2-6 AM next day)
        next_day = stream_ended_at.date() + timedelta(days=1)
        
        # Prefer 3 AM for overnight uploads (low competition)
        optimal_time = self.timezone.localize(datetime.combine(next_day, time(3, 0)))
        
        # If stream ended very late, push to next day
        if stream_ended_at.hour >= 23:
            optimal_time += timedelta(days=1)
        
        return optimal_time
    
    def get_optimal_shorts_schedule(self, base_date: datetime, num_clips: int) -> List[ScheduleSlot]:
        """
        Get optimal schedule for multiple shorts from a stream
        
        Args:
            base_date: Base date to schedule from (typically day after stream)
            num_clips: Number of clips to schedule
            
        Returns:
            List of optimal schedule slots
        """
        # Ensure timezone awareness
        if base_date.tzinfo is None:
            base_date = self.timezone.localize(base_date)
        else:
            base_date = base_date.astimezone(self.timezone)
        
        schedule_slots = []
        
        # Start scheduling from the day after the stream
        current_date = base_date.date()
        
        for clip_index in range(num_clips):
            # Spread clips across multiple days for better exposure
            days_offset = clip_index // 4  # Max 4 clips per day
            clip_date = current_date + timedelta(days=days_offset)
            
            # Get time slot for this clip
            slot_index = clip_index % 4
            optimal_slots = self._get_day_time_slots(clip_date, 'shorts')
            
            if slot_index < len(optimal_slots):
                slot = optimal_slots[slot_index]
                schedule_slots.append(slot)
            else:
                # Fallback to standard time
                fallback_time = self.timezone.localize(
                    datetime.combine(clip_date, time(19, 0))  # 7 PM fallback
                )
                schedule_slots.append(ScheduleSlot(
                    datetime=fallback_time,
                    priority=4,
                    slot_type='standard',
                    reason='Fallback time slot',
                    content_type='short'
                ))
        
        return schedule_slots
    
    def _get_day_time_slots(self, date_obj: datetime.date, content_type: str) -> List[ScheduleSlot]:
        """
        Get optimal time slots for a specific day and content type
        
        Args:
            date_obj: Date to get slots for
            content_type: 'shorts' or 'vod'
            
        Returns:
            List of ScheduleSlot objects for the day
        """
        weekday = date_obj.weekday()
        day_priority = self.day_priorities[content_type].get(weekday, 3)
        
        slots = []
        prime_times = self.prime_times.get(content_type, [])
        
        for time_start, time_end in prime_times:
            # Use middle of time window
            slot_time = time(
                hour=(time_start.hour + time_end.hour) // 2,
                minute=30  # Add some randomization
            )
            
            slot_datetime = self.timezone.localize(datetime.combine(date_obj, slot_time))
            
            # Determine slot priority and type
            if day_priority <= 2:
                slot_type = 'prime'
                priority = 1
            elif day_priority == 3:
                slot_type = 'good' 
                priority = 2
            else:
                slot_type = 'standard'
                priority = 3
            
            slots.append(ScheduleSlot(
                datetime=slot_datetime,
                priority=priority,
                slot_type=slot_type,
                reason=f"{slot_type.title()} time on {date_obj.strftime('%A')}",
                content_type=content_type
            ))
        
        # Sort by priority (best first)
        slots.sort(key=lambda x: x.priority)
        return slots
    
    def schedule_content_uploads(self, stream_id: str) -> Dict[str, List[ScheduleSlot]]:
        """
        Create complete upload schedule for a stream's content
        
        Args:
            stream_id: Database stream ID
            
        Returns:
            Dictionary with 'vod' and 'shorts' schedules
        """
        try:
            # Get stream data
            stream_data = self.db.get_stream(stream_id)
            if not stream_data:
                logger.error(f"Stream not found: {stream_id}")
                return {'vod': [], 'shorts': []}
            
            stream_ended = stream_data['ended_at']
            if isinstance(stream_ended, str):
                stream_ended = parser.parse(stream_ended)
            
            # Schedule VOD upload
            vod_time = self.get_optimal_vod_time(stream_ended)
            vod_schedule = [ScheduleSlot(
                datetime=vod_time,
                priority=1,
                slot_type='prime',
                reason='Overnight upload for reduced competition',
                content_type='vod'
            )]
            
            # Get number of clips ready for upload
            clips_count = self._count_pending_clips(stream_id)
            
            # Schedule shorts uploads
            shorts_schedule = []
            if clips_count > 0:
                # Start scheduling from day after stream
                base_date = stream_ended + timedelta(days=1)
                shorts_schedule = self.get_optimal_shorts_schedule(base_date, clips_count)
            
            logger.info(f"Scheduled uploads for stream {stream_id}: VOD at {vod_time}, {len(shorts_schedule)} shorts")
            
            return {
                'vod': vod_schedule,
                'shorts': shorts_schedule
            }
            
        except Exception as e:
            logger.error(f"Error scheduling uploads for stream {stream_id}: {e}")
            return {'vod': [], 'shorts': []}
    
    def _count_pending_clips(self, stream_id: str) -> int:
        """Count clips ready for upload from a stream"""
        try:
            # Query uploads table for clips from this stream
            uploads = self.db.supabase.table('uploads').select('*').eq('metadata->>stream_id', stream_id).eq('content_type', 'short').eq('status', 'ready_for_upload').execute()
            return len(uploads.data)
        except Exception as e:
            logger.error(f"Error counting pending clips: {e}")
            return 0
    
    def update_upload_schedule(self, upload_id: str, scheduled_time: datetime):
        """
        Update an upload record with its scheduled time
        
        Args:
            upload_id: Upload record ID
            scheduled_time: When to publish the upload
        """
        try:
            # Update the upload record with schedule info
            self.db.supabase.table('uploads').update({
                'scheduled_publish_at': scheduled_time.isoformat(),
                'status': 'scheduled'
            }).eq('id', upload_id).execute()
            
            logger.info(f"Scheduled upload {upload_id} for {scheduled_time}")
            
        except Exception as e:
            logger.error(f"Error updating upload schedule: {e}")
    
    def get_ready_to_publish(self) -> List[Dict]:
        """
        Get uploads that are ready to be published based on their schedule
        
        Returns:
            List of upload records ready for publishing
        """
        try:
            now = datetime.now(self.timezone)
            
            # Find uploads scheduled for now or earlier
            uploads = self.db.supabase.table('uploads').select('*').lte('scheduled_publish_at', now.isoformat()).eq('status', 'scheduled').execute()
            
            return uploads.data
            
        except Exception as e:
            logger.error(f"Error getting ready uploads: {e}")
            return []
    
    def get_schedule_summary(self, days_ahead: int = 7) -> Dict[str, List[Dict]]:
        """
        Get summary of scheduled content for the next N days
        
        Args:
            days_ahead: Number of days to look ahead
            
        Returns:
            Dictionary with schedule summary
        """
        try:
            now = datetime.now(self.timezone)
            future_date = now + timedelta(days=days_ahead)
            
            uploads = self.db.supabase.table('uploads').select('*').gte('scheduled_publish_at', now.isoformat()).lte('scheduled_publish_at', future_date.isoformat()).order('scheduled_publish_at').execute()
            
            schedule_by_day = {}
            
            for upload in uploads.data:
                if upload.get('scheduled_publish_at'):
                    pub_date = parser.parse(upload['scheduled_publish_at']).date()
                    day_key = pub_date.strftime('%Y-%m-%d')
                    
                    if day_key not in schedule_by_day:
                        schedule_by_day[day_key] = []
                    
                    schedule_by_day[day_key].append({
                        'id': upload['id'],
                        'type': upload.get('content_type', 'unknown'),
                        'title': upload.get('youtube_title', 'Untitled'),
                        'scheduled_time': upload['scheduled_publish_at'],
                        'status': upload['status']
                    })
            
            return schedule_by_day
            
        except Exception as e:
            logger.error(f"Error getting schedule summary: {e}")
            return {}

# Test function
def test_scheduling_optimizer():
    """Test the scheduling optimizer functionality"""
    optimizer = SchedulingOptimizer()
    
    # Test VOD scheduling
    stream_end = datetime.now() - timedelta(hours=2)
    vod_time = optimizer.get_optimal_vod_time(stream_end)
    logger.info(f"✅ VOD scheduled for: {vod_time}")
    
    # Test shorts scheduling
    base_date = datetime.now() + timedelta(days=1)
    shorts_schedule = optimizer.get_optimal_shorts_schedule(base_date, 3)
    
    logger.info(f"✅ Shorts schedule ({len(shorts_schedule)} slots):")
    for slot in shorts_schedule:
        logger.info(f"  - {slot.datetime.strftime('%Y-%m-%d %H:%M')} ({slot.slot_type}) - {slot.reason}")
    
    logger.info("✅ Scheduling optimizer test completed")

if __name__ == "__main__":
    test_scheduling_optimizer()