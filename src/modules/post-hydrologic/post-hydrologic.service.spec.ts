import { Test, TestingModule } from '@nestjs/testing';
import { PostHydrologicService } from './post-hydrologic.service';

describe('PostHydrologicService', () => {
  let service: PostHydrologicService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [PostHydrologicService],
    }).compile();

    service = module.get<PostHydrologicService>(PostHydrologicService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
